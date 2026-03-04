import "package:flutter/material.dart";
import "package:shared_preferences/shared_preferences.dart";

import "config/app_config.dart";
import "pages/auth_page.dart";
import "pages/home_page.dart";
import "services/api_client.dart";

const _authTokenKey = "auth_token";
const _networkNoticeShownKey = "network_notice_shown_v1";

void main() {
  WidgetsFlutterBinding.ensureInitialized();
  runApp(const FundQuantMobileApp());
}

class FundQuantMobileApp extends StatefulWidget {
  const FundQuantMobileApp({super.key});

  @override
  State<FundQuantMobileApp> createState() => _FundQuantMobileAppState();
}

class _FundQuantMobileAppState extends State<FundQuantMobileApp> {
  late final ApiClient _apiClient;
  bool _checkingSession = true;
  bool _loggedIn = false;

  @override
  void initState() {
    super.initState();
    _apiClient = ApiClient(baseUrl: AppConfig.apiBaseUrl);
    _restoreSession();
    _showNetworkNoticeIfNeeded();
  }

  Future<void> _showNetworkNoticeIfNeeded() async {
    final prefs = await SharedPreferences.getInstance();
    final shown = prefs.getBool(_networkNoticeShownKey) ?? false;
    if (shown || !mounted) {
      return;
    }

    WidgetsBinding.instance.addPostFrameCallback((_) async {
      if (!mounted) {
        return;
      }
      await showDialog<void>(
        context: context,
        barrierDismissible: false,
        builder: (dialogContext) {
          return AlertDialog(
            title: const Text("网络访问提示"),
            content: const Text(
              "应用需要网络连接以完成登录、获取行情和同步数据。请确认允许应用联网。",
            ),
            actions: [
              FilledButton(
                onPressed: () => Navigator.of(dialogContext).pop(),
                child: const Text("同意并继续"),
              ),
            ],
          );
        },
      );

      final latestPrefs = await SharedPreferences.getInstance();
      await latestPrefs.setBool(_networkNoticeShownKey, true);
    });
  }

  Future<void> _restoreSession() async {
    final prefs = await SharedPreferences.getInstance();
    final token = prefs.getString(_authTokenKey);
    if (token == null || token.trim().isEmpty) {
      if (!mounted) {
        return;
      }
      setState(() {
        _checkingSession = false;
        _loggedIn = false;
      });
      return;
    }

    _apiClient.setAuthToken(token);
    try {
      await _apiClient.fetchMe();
      if (!mounted) {
        return;
      }
      setState(() {
        _checkingSession = false;
        _loggedIn = true;
      });
    } catch (_) {
      _apiClient.setAuthToken(null);
      await prefs.remove(_authTokenKey);
      if (!mounted) {
        return;
      }
      setState(() {
        _checkingSession = false;
        _loggedIn = false;
      });
    }
  }

  Future<void> _handleAuthenticated(AuthSession session) async {
    _apiClient.setAuthToken(session.token);
    final prefs = await SharedPreferences.getInstance();
    await prefs.setString(_authTokenKey, session.token);
    if (!mounted) {
      return;
    }
    setState(() {
      _loggedIn = true;
    });
  }

  Future<void> _handleLogout() async {
    try {
      await _apiClient.logout();
    } catch (_) {
      // ignore network errors during local sign-out
    }
    _apiClient.setAuthToken(null);
    final prefs = await SharedPreferences.getInstance();
    await prefs.remove(_authTokenKey);
    if (!mounted) {
      return;
    }
    setState(() {
      _loggedIn = false;
    });
  }

  @override
  Widget build(BuildContext context) {
    final baseTheme = ThemeData(
      useMaterial3: true,
      colorSchemeSeed: Colors.blue,
    );

    return MaterialApp(
      title: "Fund Quant Mobile",
      debugShowCheckedModeBanner: false,
      theme: baseTheme.copyWith(
        textTheme: baseTheme.textTheme.apply(fontSizeFactor: 0.92),
      ),
      home: _checkingSession
          ? const _AppLaunchingPage()
          : (_loggedIn
              ? HomePage(apiClient: _apiClient, onLogout: _handleLogout)
              : AuthPage(
                  apiClient: _apiClient,
                  onAuthenticated: _handleAuthenticated)),
    );
  }
}

class _AppLaunchingPage extends StatelessWidget {
  const _AppLaunchingPage();

  @override
  Widget build(BuildContext context) {
    return const Scaffold(
      body: Center(child: CircularProgressIndicator()),
    );
  }
}
