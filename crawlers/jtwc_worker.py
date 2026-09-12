"""
JTWC Worker - P1
Covers: West Pacific (WP), Indian Ocean (IO), South Pacific (SP)
Source: https://www.metoc.dc3n.navy.mil/jtwc/products/
Polling: active warnings 30 min, best track 1x/day

JTWC publishes text advisories in the ATCF format and also provides
a products directory listing active storms.

ATCF deck format (A-deck / B-deck columns, space-separated):
  BASIN, CY, YYYYMMDDHH, TECHNUM, TECH, TAU, LAT, LON, VMAX, MSLP, TY, ...

This parser reads the current warnings index and individual storm files.
"""
import logging
import re
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple

from core.database import SessionLocal
from core.redis import cache_invalidate_storms
from crawlers.base_worker import (
    HTTP_SESSION,
    save_raw_file,
    sha256_of_bytes,
    sync_log,
)
from models.storm_models import Storm, StormTrack, StormTrackPoint
from utils.geo import bearing_to_text
from utils.units import categorize_storm

logger = logging.getLogger(__name__)

SOURCE = "JTWC"

# RSS là chỗ DUY NHẤT JTWC công bố danh sách bão đang có cảnh báo:
# thư mục /products/ trả 403, không có index nào khác.
JTWC_RSS_URL = "https://www.metoc.navy.mil/jtwc/rss/jtwc.rss?tropics"
JTWC_PRODUCTS_BASE = "https://www.metoc.navy.mil/jtwc/products/"

# Tiền tố file .tcw → basin. CHỈ nhận ba vùng của JTWC.
#
# ep/cp/al cũng xuất hiện trong RSS này (JTWC nhắc lại cảnh báo của NHC) nhưng
# phải bỏ qua: nhc_worker đã nuốt chúng rồi, ingest tiếp là sinh bão trùng mà
# _deduplicate_active() bên api/storms.py phải đi dọn.
JTWC_BASINS = {"wp": "WP", "io": "IO", "sh": "SH"}

# Cường độ đọc từ câu chữ trong cảnh báo → mã ty của ATCF, để dùng lại
# ATCF_TY_CATEGORIES. Xếp dài trước ngắn sau: "SUPER TYPHOON" phải thắng "TYPHOON".
JTWC_PROSE_TY = (
    ("SUPER TYPHOON", "ST"),
    ("SUBTROPICAL STORM", "SS"),
    ("TROPICAL DEPRESSION", "TD"),
    ("TROPICAL STORM", "TS"),
    ("TROPICAL CYCLONE", "TC"),
    ("TYPHOON", "TY"),
)

JTWC_UA = {"User-Agent": "Mozilla/5.0 (compatible; WeatherTracking/1.0)"}

# Significant Tropical Weather Advisory — nơi JTWC công bố các nhiễu động chưa
# đủ cấp để có cảnh báo riêng (INVEST). Không đọc hai file này thì app mù hoàn
# toàn với mọi thứ chưa thành áp thấp: ví dụ INVEST 98W ngày 12/09/2026 nằm cách
# Đà Nẵng 147 hải lý về phía đông và được dự báo men theo bờ biển Việt Nam —
# không một nguồn nào khác trong hệ thống nhìn thấy nó.
JTWC_ADVISORY_URLS = (
    JTWC_PRODUCTS_BASE + "abpwweb.txt",   # Tây Bắc TBD + Nam TBD
    JTWC_PRODUCTS_BASE + "abioweb.txt",   # Ấn Độ Dương
)

# Chữ cái cuối mã INVEST cho biết vùng. Bỏ qua E/C/L vì đó là sân của NHC.
INVEST_BASIN_BY_SUFFIX = {"W": "WP", "A": "IO", "B": "IO", "S": "SH", "P": "SH"}

ATCF_TY_CATEGORIES = {
    "TD": "tropical_depression",
    "TS": "tropical_storm",
    "TY": "typhoon",
    "ST": "violent_typhoon",
    "TC": "typhoon",
    "HU": "category_1",
    "SD": "tropical_depression",
    "SS": "tropical_storm",
    "EX": "extratropical",
    "LO": "low",
    "WV": "tropical_wave",
    "ET": "extratropical",
    "XX": "unknown",
}


def _parse_atcf_lat(s: str) -> Optional[float]:
    """Parse ATCF lat like '152N' or '85S'."""
    s = s.strip()
    if not s:
        return None
    try:
        if s.endswith("N"):
            return float(s[:-1]) / 10.0
        if s.endswith("S"):
            return -float(s[:-1]) / 10.0
        return float(s) / 10.0
    except ValueError:
        return None


def _parse_atcf_lon(s: str) -> Optional[float]:
    """Parse ATCF lon like '1324E' or '1786W'."""
    s = s.strip()
    if not s:
        return None
    try:
        if s.endswith("E"):
            return float(s[:-1]) / 10.0
        if s.endswith("W"):
            return -float(s[:-1]) / 10.0
        return float(s) / 10.0
    except ValueError:
        return None


def _parse_atcf_time(dtg: str) -> Optional[datetime]:
    """Parse ATCF 10-digit DTG YYYYMMDDHH."""
    dtg = dtg.strip()
    if len(dtg) < 10:
        return None
    try:
        return datetime.strptime(dtg[:10], "%Y%m%d%H").replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def _discover_active_storm_ids() -> List[str]:
    """
    Đọc RSS JTWC, trả về id file dạng ["wp2126", "io0326", "sh0126"].

    Vì sao không dùng ftp.nhc.noaa.gov/atcf/btk/ như trước: thư mục đó CHỈ chứa
    al/cp/ep — các vùng do NHC phụ trách. Nó chưa bao giờ và sẽ không bao giờ có
    bwp*/bio*/bsh*. Code cũ tìm không thấy file nào rồi `return None` kèm log
    "basin is clear", và vì không ném exception nên sync_log ghi nhận THÀNH CÔNG.
    Đó là lý do /api/v1/health báo JTWC xanh suốt nhiều tháng trong khi nó không
    đóng góp nổi một cơn bão nào, và Tây Bắc Thái Bình Dương / Ấn Độ Dương /
    Nam Bán Cầu trống trơn trên bản đồ.
    """
    try:
        resp = HTTP_SESSION.get(JTWC_RSS_URL, timeout=20, headers=JTWC_UA)
        resp.raise_for_status()
    except Exception as exc:
        logger.warning(f"[JTWC] RSS fetch failed: {exc}")
        return []

    ids = set()
    for fid in re.findall(r"/products/([a-z]{2}\d{4})(?:web\.txt|\.tcw)", resp.text, re.I):
        fid = fid.lower()
        if fid[:2] in JTWC_BASINS:
            ids.add(fid)
    return sorted(ids)


def _prose_ty(text: str) -> str:
    upper = text.upper()
    for phrase, code in JTWC_PROSE_TY:
        if phrase in upper:
            return code
    return ""


def _parse_tcw(text: str, file_id: str) -> Tuple[List[dict], Optional[float], Optional[float]]:
    """
    Bóc file .tcw thành danh sách point theo đúng shape mà _upsert_jtwc_storms
    đang ăn, kèm hướng và tốc độ di chuyển lấy thẳng từ header.

    .tcw mở đầu bằng một khối đã máy-đọc-được, gọn hơn hẳn phần văn xuôi:

        2026091206 14E NORBERT    011  02 275 09 SATL 030
        T000 170N 1270W 050 R050 000 NE QD ...
        T012 174N 1288W 055 ...
        T120 156N 1481W 045 ...

    Dòng đầu: DTG, id bão, tên, số hiệu cảnh báo, ..., hướng 275 độ, tốc độ 09 kt.
    T000 là vị trí hiện tại (→ tech BEST), T012 trở đi là dự báo (→ tech OFCL).
    Toạ độ mã hoá theo phần mười độ y hệt ATCF nên _parse_atcf_lat/lon dùng lại
    được nguyên vẹn.
    """
    basin = JTWC_BASINS[file_id[:2]]
    cy = file_id[2:4]

    base_time = None
    dir_deg = speed_kt = None
    for line in text.splitlines():
        m = re.match(r"^\s*(\d{10})\s+\S+", line)
        if m:
            base_time = _parse_atcf_time(m.group(1))
            mv = re.search(r"\s(\d{3})\s+(\d{2})\s+[A-Z]{3,5}\s+\d{3}\s*$", line)
            if mv:
                dir_deg = float(mv.group(1))
                speed_kt = float(mv.group(2))
            break

    if base_time is None:
        logger.warning(f"[JTWC] {file_id}: no DTG header in .tcw")
        return [], None, None

    name = ""
    nm = re.search(r"\b\d{2}[A-Z]\s+\(([A-Z][A-Z\- ]*)\)", text)
    if nm:
        candidate = nm.group(1).strip()
        if candidate not in ("INVEST", "UNKNOWN", "UNNAMED"):
            name = candidate

    ty = _prose_ty(text)

    points: List[dict] = []
    for tau_s, lat_s, lon_s, vmax_s in re.findall(
        r"^T(\d{3})\s+(\d+[NS])\s+(\d+[EW])\s+(\d+)", text, re.M
    ):
        lat = _parse_atcf_lat(lat_s)
        lon = _parse_atcf_lon(lon_s)
        if lat is None or lon is None:
            continue
        tau = int(tau_s)
        points.append(
            {
                "basin": basin,
                "cy": cy,
                "valid_time": base_time + timedelta(hours=tau),
                "tau": tau,
                "lat": lat,
                "lon": lon,
                "vmax_kt": float(vmax_s) if vmax_s else None,
                # .tcw không mang áp suất tâm — cột đó của JTWC bỏ trống.
                "mslp_hpa": None,
                "ty": ty,
                "tech": "BEST" if tau == 0 else "OFCL",
                "name": name,
            }
        )

    return points, dir_deg, speed_kt


def _parse_disturbances(text: str, advisory_dtg: Optional[datetime]) -> List[dict]:
    """
    Bóc các INVEST từ bản tin ABPW/ABIO.

    Mỗi nhiễu động là một đoạn đánh số "(1)", "(2)" trong phần TROPICAL
    DISTURBANCE SUMMARY, dạng:

        (1) THE AREA OF CONVECTION (INVEST 98W) PREVIOUSLY LOCATED NEAR
        15.4N 109.3E IS NOW LOCATED NEAR 15.6N 110.7E, APPROXIMATELY 147 NM
        EAST OF DA NANG, VIETNAM. ... MAXIMUM SUSTAINED SURFACE WINDS ARE
        ESTIMATED AT 15 TO 20 KNOTS.

    Hai chi tiết dễ sai:
      - Câu chữ xuống dòng giữa chừng nên phải gộp khoảng trắng TRƯỚC khi khớp,
        không thì "NEAR\n15.4N" trượt.
      - Trong một đoạn có thể có nhiều toạ độ ("PREVIOUSLY LOCATED NEAR X IS NOW
        LOCATED NEAR Y"). Luôn lấy cái CUỐI — đó mới là vị trí hiện tại.
    """
    flat = re.sub(r"\s+", " ", text)
    results: List[dict] = []

    for para in re.split(r"\(\d+\)", flat)[1:]:
        m = re.search(r"INVEST\s+(\d{2}[A-Z])", para)
        if not m:
            continue
        invest_id = m.group(1).upper()
        basin = INVEST_BASIN_BY_SUFFIX.get(invest_id[-1])
        if basin is None:
            continue

        fixes = re.findall(
            r"NEAR\s+(\d+\.?\d*)\s*([NS])\s+(\d+\.?\d*)\s*([EW])", para
        )
        if not fixes:
            continue
        lat_v, lat_h, lon_v, lon_h = fixes[-1]
        lat = float(lat_v) * (1 if lat_h == "N" else -1)
        lon = float(lon_v) * (1 if lon_h == "E" else -1)

        wind_kt = None
        w = re.search(
            r"WINDS ARE ESTIMATED AT (\d+) TO (\d+) KNOTS", para
        )
        if w:
            # Lấy cận trên: JTWC nói "15 TO 20 KNOTS" nghĩa là gió mạnh nhất ~20.
            wind_kt = float(w.group(2))

        results.append(
            {
                "basin": basin,
                "cy": invest_id[:2],
                "valid_time": advisory_dtg or datetime.now(timezone.utc),
                "tau": 0,
                "lat": lat,
                "lon": lon,
                "vmax_kt": wind_kt,
                "mslp_hpa": None,
                "ty": "",
                "tech": "BEST",
                "name": f"Invest {invest_id}",
                "is_invest": True,
                "invest_id": invest_id,
            }
        )

    return results


def _fetch_disturbances() -> Tuple[Dict[str, List[dict]], List[bytes]]:
    """Đọc ABPW + ABIO, trả về {storm_key: [point]} cho mọi INVEST đang mở."""
    out: Dict[str, List[dict]] = {}
    blobs: List[bytes] = []

    for url in JTWC_ADVISORY_URLS:
        try:
            r = HTTP_SESSION.get(url, timeout=20, headers=JTWC_UA)
            r.raise_for_status()
        except Exception as exc:
            logger.warning(f"[JTWC] {url} fetch failed: {exc}")
            continue

        blobs.append(r.content)

        dtg = None
        head = re.match(r"^\s*\w{6}\s+\w{4}\s+(\d{6})", r.text)
        if head:
            # Header mang DDHHMM, không có tháng/năm — ghép với tháng hiện tại.
            now = datetime.now(timezone.utc)
            try:
                dtg = now.replace(
                    day=int(head.group(1)[:2]),
                    hour=int(head.group(1)[2:4]),
                    minute=int(head.group(1)[4:6]),
                    second=0, microsecond=0,
                )
            except ValueError:
                dtg = None

        for pt in _parse_disturbances(r.text, dtg):
            out[f"INVEST{pt['invest_id']}"] = [pt]

    return out, blobs


def run_jtwc_fetch_active():
    """Kéo bão đang hoạt động (WP/IO/SH) và các INVEST từ sản phẩm của JTWC."""
    with sync_log(SOURCE, "jtwc_fetch_active_warnings") as log_data:
        file_ids = _discover_active_storm_ids()
        storm_data: Dict[str, List[dict]] = {}
        movements: Dict[str, Tuple[Optional[float], Optional[float]]] = {}
        blobs: List[bytes] = []

        for fid in file_ids:
            url = f"{JTWC_PRODUCTS_BASE}{fid}.tcw"
            try:
                r = HTTP_SESSION.get(url, timeout=20, headers=JTWC_UA)
                r.raise_for_status()
            except Exception as exc:
                logger.warning(f"[JTWC] {fid}.tcw fetch failed: {exc}")
                continue

            blobs.append(r.content)
            points, dir_deg, speed_kt = _parse_tcw(r.text, fid)
            if not points:
                continue

            storm_key = f"{JTWC_BASINS[fid[:2]]}{fid[2:4]}"
            storm_data[storm_key] = points
            movements[storm_key] = (dir_deg, speed_kt)

        if file_ids and not storm_data:
            # RSS có tên bão nhưng không bóc được điểm nào → nguồn đổi định dạng.
            # Ném lỗi để sync_log ghi FAIL và /api/v1/health kêu, thay vì lặng lẽ
            # báo thành công như bug cũ.
            raise RuntimeError(
                f"[JTWC] RSS liệt kê {len(file_ids)} bão nhưng không .tcw nào bóc được điểm"
            )

        # INVEST chạy kể cả khi không có cơn bão nào có cảnh báo — đó chính là
        # tình huống thường gặp nhất, và cũng là lúc bản đồ trông trống trơn.
        invests, invest_blobs = _fetch_disturbances()
        storm_data.update(invests)
        blobs.extend(invest_blobs)

        logger.info(
            f"[JTWC] {len(file_ids)} bão có cảnh báo, {len(invests)} nhiễu động: "
            f"{sorted(storm_data)}"
        )

        if not storm_data:
            logger.info("[JTWC] Không có hệ thống nào ở WP/IO/SH")
            log_data["records_processed"] = 0
            return

        raw = b"\n".join(blobs)
        log_data["checksum"] = sha256_of_bytes(raw)
        log_data["raw_file_path"] = save_raw_file(SOURCE, "jtwc_tcw", "txt", raw)
        log_data["records_processed"] = len(storm_data)

        db = SessionLocal()
        try:
            _upsert_jtwc_storms(storm_data, db, movements=movements)
            db.commit()
        finally:
            db.close()

    cache_invalidate_storms()


def _upsert_jtwc_storms(storm_data: Dict[str, List[dict]], db, movements=None):
    """Ghi bão bóc từ .tcw xuống DB."""
    movements = movements or {}
    active_keys = set()

    for storm_key, points in storm_data.items():
        if not points:
            continue

        # KHÔNG lọc theo năm nữa. Bộ lọc cũ (valid_time.year == current_year) có
        # lý do khi nguồn là b-deck nhiều mùa, còn .tcw chỉ chứa đúng một bản tin
        # hiện hành. Giữ lại thì mỗi giao thừa nó lại cắt cụt dự báo T072–T120 —
        # đúng giữa mùa bão Nam Bán Cầu (tháng 11 → tháng 4).
        observed = [p for p in points if p["tech"] == "BEST"]
        forecast = [p for p in points if p["tech"] == "OFCL"]
        latest = observed[-1] if observed else points[0]

        basin_code = _map_basin(latest["basin"])
        source_id = storm_key.lower()

        storm = (
            db.query(Storm)
            .filter(Storm.source == SOURCE, Storm.source_storm_id == source_id)
            .first()
        )
        if storm is None:
            storm = Storm(source=SOURCE, source_storm_id=source_id)
            db.add(storm)
            db.flush()

        names = [p["name"] for p in points if p.get("name")]
        if names:
            storm.name = names[-1].title()
        elif not storm.name:
            storm.name = storm_key.upper()

        storm.basin = basin_code
        storm.is_active = True
        # INVEST chưa phải bão: đánh dấu riêng để app hiển thị đúng bản chất
        # "vùng nhiễu động đang theo dõi" thay vì gọi nhầm nó là áp thấp.
        storm.status = "invest" if latest.get("is_invest") else "active"
        storm.lat = latest["lat"]
        storm.lon = latest["lon"]
        storm.wind_kt = latest["vmax_kt"]
        storm.pressure_hpa = latest["mslp_hpa"]
        storm.last_update_utc = datetime.now(timezone.utc)

        if latest.get("is_invest"):
            storm.category = "invest"
        else:
            storm.category = ATCF_TY_CATEGORIES.get(latest["ty"]) or (
                categorize_storm(latest["vmax_kt"], basin_code) if latest["vmax_kt"] else None
            )

        # Hướng/tốc độ lấy thẳng từ header .tcw — đó là số JTWC tự công bố,
        # chính xác hơn suy ngược từ hai vị trí cách nhau 6 tiếng.
        dir_deg, speed_kt = movements.get(storm_key, (None, None))
        if dir_deg is not None:
            storm.movement_direction_deg = dir_deg
            storm.movement_direction_text = bearing_to_text(dir_deg)
            storm.movement_speed_kt = speed_kt

        _append_observed_point(storm, latest, db)
        _rebuild_forecast_track(storm, forecast, db)

        active_keys.add(source_id)

    (
        db.query(Storm)
        .filter(
            Storm.source == SOURCE,
            Storm.is_active == True,
            ~Storm.source_storm_id.in_(active_keys),
        )
        .update({"is_active": False, "status": "dissipated"}, synchronize_session=False)
    )


def _append_observed_point(storm, pt: dict, db):
    """
    Cộng dồn vệt đã đi, KHÔNG xoá-rồi-dựng-lại.

    Khác biệt quan trọng so với b-deck: .tcw chỉ mang ĐÚNG MỘT vị trí hiện tại
    (T000). Nếu dùng _rebuild_track() như cũ thì mỗi lần chạy sẽ xoá sạch lịch
    sử rồi ghi lại đúng một điểm — vệt bão vĩnh viễn không bao giờ dài quá một
    chấm. Lịch sử ở đây phải tích luỹ qua từng lần crawl (30 phút/lần).
    """
    exists = (
        db.query(StormTrackPoint)
        .filter(
            StormTrackPoint.storm_id == storm.id,
            StormTrackPoint.point_type == "observed",
            StormTrackPoint.valid_time_utc == pt["valid_time"],
        )
        .first()
    )
    if exists is None:
        db.add(
            StormTrackPoint(
                storm_id=storm.id,
                point_type="observed",
                valid_time_utc=pt["valid_time"],
                lat=pt["lat"],
                lon=pt["lon"],
                wind_kt=pt["vmax_kt"],
                pressure_hpa=pt["mslp_hpa"],
                category=ATCF_TY_CATEGORIES.get(pt["ty"]),
                movement_direction_deg=storm.movement_direction_deg,
                movement_direction_text=storm.movement_direction_text,
                movement_speed_kt=storm.movement_speed_kt,
            )
        )
        db.flush()

    rows = (
        db.query(StormTrackPoint)
        .filter(
            StormTrackPoint.storm_id == storm.id,
            StormTrackPoint.point_type == "observed",
        )
        .order_by(StormTrackPoint.valid_time_utc)
        .all()
    )
    _replace_linestring(storm, "observed", [[r.lon, r.lat] for r in rows], db)


def _rebuild_forecast_track(storm, points: list, db):
    """
    Vệt dự báo thì ngược lại: xoá sạch rồi dựng lại mỗi lần.

    Bản tin mới của JTWC thay thế hoàn toàn bản cũ, giữ lại điểm dự báo cũ là
    vẽ ra hai đường mâu thuẫn nhau trên cùng một bản đồ.
    """
    db.query(StormTrackPoint).filter(
        StormTrackPoint.storm_id == storm.id,
        StormTrackPoint.point_type == "forecast",
    ).delete()

    coords = []
    for pt in sorted(points, key=lambda p: p["tau"]):
        coords.append([pt["lon"], pt["lat"]])
        db.add(
            StormTrackPoint(
                storm_id=storm.id,
                point_type="forecast",
                valid_time_utc=pt["valid_time"],
                forecast_hour=pt["tau"],
                lat=pt["lat"],
                lon=pt["lon"],
                wind_kt=pt["vmax_kt"],
                pressure_hpa=pt["mslp_hpa"],
                category=ATCF_TY_CATEGORIES.get(pt["ty"]),
            )
        )

    _replace_linestring(storm, "forecast", coords, db)


def _replace_linestring(storm, track_type: str, coords: list, db):
    db.query(StormTrack).filter(
        StormTrack.storm_id == storm.id,
        StormTrack.track_type == track_type,
    ).delete()
    if len(coords) >= 2:
        db.add(
            StormTrack(
                storm_id=storm.id,
                track_type=track_type,
                geojson={"type": "LineString", "coordinates": coords},
            )
        )


def _map_basin(atcf_basin: str) -> str:
    mapping = {
        "WP": "WP",
        "IO": "IO",
        "SH": "SP",
        "SP": "SP",
        "AL": "AL",
        "EP": "EP",
        "CP": "CP",
    }
    return mapping.get(atcf_basin.upper(), atcf_basin.upper())


def run_jtwc_crawler():
    logger.info("[JTWC] Starting crawler")
    try:
        run_jtwc_fetch_active()
    except Exception as e:
        logger.error(f"[JTWC] Crawler error: {e}")

    # Fetch satellite images for recently updated storms
    from crawlers.satellite_image_worker import fetch_and_store_for_storm
    db = SessionLocal()
    try:
        active_storms = db.query(Storm).filter(Storm.source == "JTWC", Storm.is_active == True).all()
        for storm in active_storms:
            try:
                n_imgs = fetch_and_store_for_storm(storm)
                if n_imgs:
                    logger.info(f"[JTWC] Stored {n_imgs} satellite image(s) for {storm.source_storm_id}")
            except Exception as e:
                logger.warning(f"[JTWC] Satellite image fetch failed for {storm.source_storm_id}: {e}")
    finally:
        db.close()

    logger.info("[JTWC] Done.")
