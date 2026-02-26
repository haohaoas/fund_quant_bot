import "dart:convert";

import "package:http/http.dart" as http;

import "../models/account.dart";
import "../models/market_index.dart";
import "../models/portfolio.dart";
import "../models/recommendations.dart";
import "../models/sector_flow.dart";
import "../models/watchlist.dart";

class AuthUser {
  const AuthUser({
    required this.id,
    required this.email,
    required this.createdAt,
    required this.updatedAt,
  });

  final int id;
  final String email;
  final String createdAt;
  final String updatedAt;

  factory AuthUser.fromJson(Map<String, dynamic> json) {
    int toInt(dynamic value, [int fallback = 0]) {
      if (value == null) {
        return fallback;
      }
      if (value is num) {
        return value.toInt();
      }
      return int.tryParse(value.toString()) ?? fallback;
    }

    String toText(dynamic value, [String fallback = ""]) {
      if (value == null) {
        return fallback;
      }
      final text = value.toString();
      return text.isEmpty ? fallback : text;
    }

    return AuthUser(
      id: toInt(json["id"]),
      email: toText(json["email"]),
      createdAt: toText(json["created_at"]),
      updatedAt: toText(json["updated_at"]),
    );
  }
}

class AuthSession {
  const AuthSession({
    required this.token,
    required this.expiresAt,
    required this.user,
  });

  final String token;
  final String expiresAt;
  final AuthUser user;

  factory AuthSession.fromJson(Map<String, dynamic> json) {
    final userMap = (json["user"] as Map?)?.cast<String, dynamic>() ??
        const <String, dynamic>{};
    return AuthSession(
      token: (json["token"] ?? "").toString(),
      expiresAt: (json["expires_at"] ?? "").toString(),
      user: AuthUser.fromJson(userMap),
    );
  }
}

class ApiClient {
  ApiClient({
    required this.baseUrl,
    http.Client? client,
    String? authToken,
  })  : _client = client ?? http.Client(),
        _authToken = authToken;

  final String baseUrl;
  final http.Client _client;
  static const Duration _requestTimeout = Duration(seconds: 12);
  String? _authToken;

  String? get authToken => _authToken;

  void setAuthToken(String? token) {
    final normalized = token?.trim();
    _authToken = (normalized == null || normalized.isEmpty) ? null : normalized;
  }

  Map<String, String> _backendHeaders([Map<String, String>? extra]) {
    final headers = <String, String>{
      if (extra != null) ...extra,
    };
    final token = _authToken;
    if (token != null && token.isNotEmpty) {
      headers["authorization"] = "Bearer $token";
    }
    return headers;
  }

  Future<AuthSession> loginWithEmail({
    required String email,
    required String password,
  }) async {
    final uri = Uri.parse("$baseUrl/api/auth/login");
    final response = await _client
        .post(
          uri,
          headers: _backendHeaders({"content-type": "application/json"}),
          body: jsonEncode(<String, dynamic>{
            "email": email.trim(),
            "password": password,
          }),
        )
        .timeout(_requestTimeout);
    _ensureSuccess(response, uri);
    final payload = _decodeMap(response.body);
    return AuthSession.fromJson(payload);
  }

  Future<AuthSession> registerWithEmail({
    required String email,
    required String password,
  }) async {
    final uri = Uri.parse("$baseUrl/api/auth/register");
    final response = await _client
        .post(
          uri,
          headers: _backendHeaders({"content-type": "application/json"}),
          body: jsonEncode(<String, dynamic>{
            "email": email.trim(),
            "password": password,
          }),
        )
        .timeout(_requestTimeout);
    _ensureSuccess(response, uri);
    final payload = _decodeMap(response.body);
    return AuthSession.fromJson(payload);
  }

  Future<AuthUser> fetchMe() async {
    final uri = Uri.parse("$baseUrl/api/auth/me");
    final response = await _client
        .get(uri, headers: _backendHeaders())
        .timeout(_requestTimeout);
    _ensureSuccess(response, uri);
    final payload = _decodeMap(response.body);
    final userMap = (payload["user"] as Map?)?.cast<String, dynamic>() ??
        const <String, dynamic>{};
    return AuthUser.fromJson(userMap);
  }

  Future<void> logout() async {
    final uri = Uri.parse("$baseUrl/api/auth/logout");
    final response = await _client
        .post(uri, headers: _backendHeaders())
        .timeout(_requestTimeout);
    _ensureSuccess(response, uri);
  }

  Future<RecommendationsResponse> fetchRecommendations({
    bool forceRefresh = false,
  }) async {
    final uri = Uri.parse("$baseUrl/api/recommendations").replace(
      queryParameters: {
        "force_refresh": forceRefresh.toString(),
      },
    );
    final response = await _client.get(uri).timeout(_requestTimeout);
    _ensureSuccess(response, uri);
    final payload = _decodeMap(response.body);
    return RecommendationsResponse.fromJson(payload);
  }

  Future<SectorFlowResponse> fetchSectorFlow({
    String indicator = "今日",
    String sectorType = "行业资金流",
    int topN = 20,
  }) async {
    final uri = Uri.parse("$baseUrl/api/sector_fund_flow").replace(
      queryParameters: {
        "indicator": indicator,
        "sector_type": sectorType,
        "top_n": "$topN",
      },
    );
    final response = await _client.get(uri).timeout(_requestTimeout);
    _ensureSuccess(response, uri);
    final payload = _decodeMap(response.body);
    return SectorFlowResponse.fromJson(payload);
  }

  Future<PortfolioResponse> fetchPortfolio({
    bool forceRefresh = false,
    int? accountId,
  }) async {
    final uri = Uri.parse("$baseUrl/api/portfolio").replace(
      queryParameters: <String, String>{
        if (accountId != null) "account_id": "$accountId",
        if (forceRefresh) "force_refresh": "true",
        if (forceRefresh)
          "_ts": DateTime.now().millisecondsSinceEpoch.toString(),
      },
    );
    final response = await _client
        .get(uri, headers: _backendHeaders())
        .timeout(_requestTimeout);
    _ensureSuccess(response, uri);
    final payload = _decodeMap(response.body);
    return PortfolioResponse.fromJson(payload);
  }

  Future<List<AppAccount>> fetchAccounts() async {
    final uri = Uri.parse("$baseUrl/api/accounts");
    final response = await _client
        .get(uri, headers: _backendHeaders())
        .timeout(_requestTimeout);
    _ensureSuccess(response, uri);
    final payload = _decodeMap(response.body);
    final items = (payload["items"] as List? ?? [])
        .whereType<Map>()
        .map((item) => AppAccount.fromJson(item.cast<String, dynamic>()))
        .toList();
    return items;
  }

  Future<AppAccount> createAccount({
    required String name,
    double initialCash = 0.0,
    String avatar = "",
  }) async {
    final uri = Uri.parse("$baseUrl/api/accounts");
    final response = await _client
        .post(
          uri,
          headers: _backendHeaders({"content-type": "application/json"}),
          body: jsonEncode(<String, dynamic>{
            "name": name.trim(),
            "cash": initialCash,
            "avatar": avatar.trim(),
          }),
        )
        .timeout(_requestTimeout);
    _ensureSuccess(response, uri);
    final payload = _decodeMap(response.body);
    final account = (payload["account"] as Map?)?.cast<String, dynamic>() ??
        <String, dynamic>{};
    return AppAccount.fromJson(account);
  }

  Future<AppAccount> updateAccount({
    required int accountId,
    String? name,
    String? avatar,
  }) async {
    final uri = Uri.parse("$baseUrl/api/accounts/$accountId");
    final body = <String, dynamic>{
      if (name != null) "name": name.trim(),
      if (avatar != null) "avatar": avatar.trim(),
    };
    final response = await _client
        .patch(
          uri,
          headers: _backendHeaders({"content-type": "application/json"}),
          body: jsonEncode(body),
        )
        .timeout(_requestTimeout);
    _ensureSuccess(response, uri);
    final payload = _decodeMap(response.body);
    final account = (payload["account"] as Map?)?.cast<String, dynamic>() ??
        <String, dynamic>{};
    return AppAccount.fromJson(account);
  }

  Future<List<WatchlistItem>> fetchWatchlist() async {
    final uri = Uri.parse("$baseUrl/api/watchlist");
    final response = await _client
        .get(uri, headers: _backendHeaders())
        .timeout(_requestTimeout);
    _ensureSuccess(response, uri);
    final payload = _decodeMap(response.body);
    final items = (payload["items"] as List? ?? [])
        .whereType<Map>()
        .map((item) => WatchlistItem.fromJson(item.cast<String, dynamic>()))
        .toList();
    return items;
  }

  Future<WatchlistItem> upsertWatchlist({
    required String code,
    String name = "",
  }) async {
    final uri = Uri.parse("$baseUrl/api/watchlist");
    final response = await _client
        .post(
          uri,
          headers: _backendHeaders({"content-type": "application/json"}),
          body: jsonEncode(<String, dynamic>{
            "code": code.trim(),
            "name": name.trim(),
          }),
        )
        .timeout(_requestTimeout);
    _ensureSuccess(response, uri);
    final payload = _decodeMap(response.body);
    final item = (payload["item"] as Map?)?.cast<String, dynamic>() ??
        <String, dynamic>{};
    return WatchlistItem.fromJson(item);
  }

  Future<void> removeWatchlist(String code) async {
    final normalizedCode = code.trim();
    final uri = Uri.parse(
      "$baseUrl/api/watchlist/${Uri.encodeComponent(normalizedCode)}",
    );
    final response = await _client
        .delete(uri, headers: _backendHeaders())
        .timeout(_requestTimeout);
    _ensureSuccess(response, uri);
  }

  Future<FundAnalysis> analyzeWatchFund({
    required String code,
    String name = "",
  }) async {
    final uri = Uri.parse("$baseUrl/api/watchlist/analyze").replace(
      queryParameters: <String, String>{
        "code": code.trim(),
        if (name.trim().isNotEmpty) "name": name.trim(),
      },
    );
    final response = await _client
        .get(uri, headers: _backendHeaders())
        .timeout(_requestTimeout);
    _ensureSuccess(response, uri);
    final payload = _decodeMap(response.body);
    return FundAnalysis.fromJson(payload);
  }

  Future<List<MarketIndexQuote>> fetchMajorIndices() async {
    const codeOrder = <String>["000001", "399001", "399006", "000300"];
    const codeNameMap = <String, String>{
      "000001": "上证指数",
      "399001": "深证成指",
      "399006": "创业板指",
      "000300": "沪深300",
    };

    final uri =
        Uri.parse("https://push2.eastmoney.com/api/qt/ulist.np/get").replace(
      queryParameters: {
        "ut": "fa5fd1943c7b386f172d6893dbfba10b",
        "fltt": "2",
        "invt": "2",
        "fields": "f12,f14,f2,f3",
        "secids": "1.000001,0.399001,0.399006,1.000300",
        "pn": "1",
        "pz": "20",
      },
    );

    final response = await _client.get(uri).timeout(_requestTimeout);
    if (response.statusCode < 200 || response.statusCode >= 300) {
      throw ApiException(
        "Request failed: ${response.statusCode} ${response.reasonPhrase}",
        uri: uri,
        body: response.body,
        statusCode: response.statusCode,
      );
    }

    final payload = _decodeMap(response.body);
    final data = (payload["data"] as Map?)?.cast<String, dynamic>() ?? {};
    final diff = (data["diff"] as List? ?? [])
        .whereType<Map>()
        .map((e) => e.cast<String, dynamic>());
    final diffByCode = <String, Map<String, dynamic>>{};
    for (final item in diff) {
      final code = (item["f12"] ?? "").toString();
      if (code.isNotEmpty) {
        diffByCode[code] = item;
      }
    }

    return codeOrder
        .map((code) => MarketIndexQuote.fromEastmoney(
              diffByCode[code] ?? const <String, dynamic>{},
              fallbackName: codeNameMap[code] ?? code,
            ))
        .toList();
  }

  Future<void> createInvestment({
    required String code,
    required double amount,
    required String action,
    double? nav,
    double? shares,
    int? accountId,
  }) async {
    final uri = Uri.parse("$baseUrl/api/investments");
    final body = <String, dynamic>{
      "code": code.trim(),
      "amount": amount,
      "action": action.trim().toUpperCase(),
      if (accountId != null) "account_id": accountId,
      if (nav != null) "nav": nav,
      if (shares != null) "shares": shares,
    };

    final response = await _client
        .post(
          uri,
          headers: _backendHeaders({"content-type": "application/json"}),
          body: jsonEncode(body),
        )
        .timeout(_requestTimeout);
    _ensureSuccess(response, uri);
  }

  Future<void> deletePosition(String code, {int? accountId}) async {
    final normalizedCode = code.trim();
    final uri = Uri.parse(
      "$baseUrl/api/positions/${Uri.encodeComponent(normalizedCode)}",
    ).replace(
      queryParameters: <String, String>{
        if (accountId != null) "account_id": "$accountId",
      },
    );
    final response = await _client
        .delete(uri, headers: _backendHeaders())
        .timeout(_requestTimeout);
    _ensureSuccess(response, uri);
  }

  static Map<String, dynamic> _decodeMap(String body) {
    final decoded = jsonDecode(body);
    if (decoded is! Map<String, dynamic>) {
      throw const FormatException("Unexpected response type");
    }
    return decoded;
  }

  static void _ensureSuccess(http.Response response, Uri uri) {
    if (response.statusCode >= 200 && response.statusCode < 300) {
      return;
    }
    throw ApiException(
      "Request failed: ${response.statusCode} ${response.reasonPhrase}",
      uri: uri,
      body: response.body,
      statusCode: response.statusCode,
    );
  }
}

class ApiException implements Exception {
  const ApiException(
    this.message, {
    required this.uri,
    required this.body,
    required this.statusCode,
  });

  final String message;
  final Uri uri;
  final String body;
  final int statusCode;

  @override
  String toString() {
    return "$message @ $uri";
  }
}
