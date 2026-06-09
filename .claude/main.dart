import 'dart:async';
import 'dart:isolate';

import 'package:flutter/foundation.dart';
import 'package:flutter/services.dart';
import 'package:flutter/widgets.dart';
import 'package:flutter_bloc/flutter_bloc.dart';
import 'common/extensions/locale_x.dart';

import 'package:talker_bloc_logger/talker_bloc_logger.dart';
import 'package:talker_flutter/talker_flutter.dart';

import 'common/theme/theme_cubit.dart';
import 'core/app/app_widget.dart';
import 'core/language/lang_cubit.dart';
import 'core/language/lang_repository_interface.dart';
import 'core/locators/di/getit_utils.dart';
import 'core/utils/logger.dart';
import 'core/utils/env_config.dart';
import 'data/local/storage.dart';
import 'features/settings/presentation/cubit/settings_cubit.dart';
import 'features/weather_map/presentation/cubit/weather_map_cubit.dart';

void main() {
  runZonedGuarded(
    () async {
      WidgetsFlutterBinding.ensureInitialized();

      // Load environment variables
      await EnvConfig.load();

      await Storage.setup();
      await GetItUtils.setup();
      // await getIt<InAppPurchaseService>().init();

      final langRepository = getIt<ILangRepository>();
      final talker = getIt<Talker>();

      _setupErrorHooks(talker);

      logger.d(
        'deviceLocale - ${langRepository.getDeviceLocale().fullLanguageCode}',
      );
      logger.d(
        'currentLocale - ${langRepository.getLocale().fullLanguageCode}',
      );

      Bloc.observer = TalkerBlocObserver(talker: talker);
      SystemChrome.setPreferredOrientations([
        DeviceOrientation.portraitUp,
        DeviceOrientation.portraitDown,
      ]);
      runApp(
        MultiBlocProvider(
          providers: [
            BlocProvider(create: (_) => getIt<ThemeCubit>()),
            BlocProvider(create: (_) => getIt<LangCubit>()),
            BlocProvider(create: (_) => getIt<SettingsCubit>()),
            BlocProvider(create: (_) => getIt<WeatherMapCubit>()..init()),
          ],
          child: const AppWidget(),
        ),
      );
    },
    (error, stack) {
      getIt<Talker>().handle(error, stack);
    },
  );
}

Future _setupErrorHooks(Talker talker, {bool catchFlutterErrors = true}) async {
  if (catchFlutterErrors) {
    FlutterError.onError = (FlutterErrorDetails details) async {
      _reportError(details.exception, details.stack, talker);
    };
  }
  PlatformDispatcher.instance.onError = (error, stack) {
    _reportError(error, stack, talker);
    return true;
  };

  /// Web doesn't have Isolate error listener support
  if (!kIsWeb) {
    Isolate.current.addErrorListener(
      RawReceivePort((dynamic pair) async {
        final isolateError = pair as List<dynamic>;
        _reportError(
          isolateError.first.toString(),
          isolateError.last.toString(),
          talker,
        );
      }).sendPort,
    );
  }
}

void _reportError(dynamic error, dynamic stackTrace, Talker talker) async {
  talker.error('Unhandled Exception', error, stackTrace);
}
