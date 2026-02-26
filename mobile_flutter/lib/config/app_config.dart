class AppConfig {
  const AppConfig._();

  static const String apiBaseUrl = String.fromEnvironment(
    "API_BASE_URL",
    // Default to online backend so release builds still work
    // even when --dart-define is omitted.
    defaultValue: "http://47.112.109.7:8000",
  );
}
