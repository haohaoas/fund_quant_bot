import "dart:convert";

import "package:http/http.dart" as http;

import "../models/account.dart";
import "../models/market_index.dart";
import "../models/news.dart";
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

class FundSearchResult {
  const FundSearchResult({
    required this.code,
    required this.name,
  });

  final String code;
  final String name;

  String get displayName => name.isEmpty ? code : "$name ($code)";
}

class FundTrendPoint {
  const FundTrendPoint({
    required this.time,
    required this.nav,
    required this.pct,
    required this.ts,
  });

  final String time;
  final double nav;
  final double? pct;
  final int? ts;

  factory FundTrendPoint.fromJson(Map<String, dynamic> json) {
    double? parseNullableDouble(dynamic value) {
      if (value == null) {
        return null;
      }
      if (value is num) {
        return value.toDouble();
      }
      return double.tryParse(value.toString());
    }

    int? parseNullableInt(dynamic value) {
      if (value == null) {
        return null;
      }
      if (value is int) {
        return value;
      }
      if (value is num) {
        return value.toInt();
      }
      return int.tryParse(value.toString());
    }

    return FundTrendPoint(
      time: (json["time"] ?? "").toString(),
      nav: (json["nav"] is num)
          ? (json["nav"] as num).toDouble()
          : double.tryParse((json["nav"] ?? "").toString()) ?? 0,
      pct: parseNullableDouble(json["pct"]),
      ts: parseNullableInt(json["ts"]),
    );
  }
}

class FundTrendResponse {
  const FundTrendResponse({
    required this.ok,
    required this.code,
    required this.name,
    required this.source,
    required this.generatedAt,
    required this.points,
    required this.error,
  });

  final bool ok;
  final String code;
  final String name;
  final String source;
  final String generatedAt;
  final List<FundTrendPoint> points;
  final String error;

  factory FundTrendResponse.fromJson(Map<String, dynamic> json) {
    final points = (json["points"] as List? ?? [])
        .whereType<Map>()
        .map((item) => FundTrendPoint.fromJson(item.cast<String, dynamic>()))
        .toList();

    return FundTrendResponse(
      ok: json["ok"] == true,
      code: (json["code"] ?? "").toString(),
      name: (json["name"] ?? "").toString(),
      source: (json["source"] ?? "").toString(),
      generatedAt: (json["generated_at"] ?? "").toString(),
      points: points,
      error: (json["error"] ?? "").toString(),
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

  String _normalizeQuoteSource(String? value) {
    final mode = (value ?? "").trim().toLowerCase();
    if (mode == "estimate" || mode == "settled" || mode == "fund123") {
      return mode;
    }
    return "auto";
  }

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

  Future<NewsListResponse> fetchNewsList({int limit = 40}) async {
    final uri = Uri.parse("$baseUrl/api/news").replace(
      queryParameters: {
        "limit": "$limit",
      },
    );
    final response = await _client.get(uri).timeout(_requestTimeout);
    _ensureSuccess(response, uri);
    final payload = _decodeMap(response.body);
    return NewsListResponse.fromJson(payload);
  }

  Future<List<FundSearchResult>> searchFunds(
    String keyword, {
    int limit = 20,
  }) async {
    final q = keyword.trim();
    if (q.isEmpty) {
      return const [];
    }

    final uri = Uri.parse(
      "https://fundsuggest.eastmoney.com/FundSearch/api/FundSearchAPI.ashx",
    ).replace(
      queryParameters: <String, String>{
        "m": "1",
        "key": q,
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

    dynamic decoded;
    try {
      decoded = jsonDecode(response.body);
    } catch (_) {
      final body = response.body;
      final left = body.indexOf("{");
      final right = body.lastIndexOf("}");
      if (left >= 0 && right > left) {
        decoded = jsonDecode(body.substring(left, right + 1));
      } else {
        throw const FormatException("Unexpected search response");
      }
    }

    if (decoded is! Map) {
      return const [];
    }
    final map = decoded.cast<String, dynamic>();
    final rawList = (map["Datas"] as List?) ??
        (map["datas"] as List?) ??
        (map["Data"] as List?) ??
        const [];

    String pickText(Map<String, dynamic> data, List<String> keys) {
      for (final key in keys) {
        final v = data[key];
        if (v == null) {
          continue;
        }
        final text = v.toString().trim();
        if (text.isNotEmpty) {
          return text;
        }
      }
      return "";
    }

    final dedup = <String, FundSearchResult>{};
    for (final raw in rawList) {
      if (raw is! Map) {
        continue;
      }
      final data = raw.cast<String, dynamic>();
      var code = pickText(data, const [
        "CODE",
        "Code",
        "code",
        "FundCode",
        "fund_code",
      ]);
      if (code.isEmpty) {
        final baseInfo =
            pickText(data, const ["FundBaseInfo", "fund_base_info"]);
        final match = RegExp(r"(\d{6})").firstMatch(baseInfo);
        if (match != null) {
          code = match.group(1) ?? "";
        }
      }
      final matchCode = RegExp(r"(\d{6})").firstMatch(code);
      code = matchCode?.group(1) ?? code;
      if (code.isEmpty) {
        continue;
      }

      var name = pickText(data, const [
        "NAME",
        "Name",
        "name",
        "SHORTNAME",
        "ShortName",
      ]);
      if (name.isEmpty) {
        final baseInfo =
            pickText(data, const ["FundBaseInfo", "fund_base_info"]);
        final fields = baseInfo
            .split(RegExp(r"[\t|,]"))
            .map((e) => e.trim())
            .where((e) => e.isNotEmpty)
            .toList();
        if (fields.isNotEmpty) {
          final nonCode = fields.firstWhere(
            (f) => !RegExp(r"^\d{6}$").hasMatch(f),
            orElse: () => "",
          );
          if (nonCode.isNotEmpty) {
            name = nonCode;
          }
        }
      }
      dedup[code] = FundSearchResult(code: code, name: name);
      if (dedup.length >= limit) {
        break;
      }
    }

    final values = dedup.values.toList();
    values.sort((a, b) => a.code.compareTo(b.code));
    return values;
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
    String quoteSource = "auto",
  }) async {
    final source = _normalizeQuoteSource(quoteSource);
    final uri = Uri.parse("$baseUrl/api/portfolio").replace(
      queryParameters: <String, String>{
        if (accountId != null) "account_id": "$accountId",
        "quote_source": source,
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

  Future<FundTrendResponse> fetchFundTrend({
    required String code,
    String quoteSource = "auto",
    int maxPoints = 180,
  }) async {
    final source = _normalizeQuoteSource(quoteSource);
    final uri = Uri.parse("$baseUrl/api/funds/trend").replace(
      queryParameters: <String, String>{
        "code": code.trim(),
        "quote_source": source,
        "max_points": "$maxPoints",
      },
    );
    final response = await _client
        .get(uri, headers: _backendHeaders())
        .timeout(_requestTimeout);
    _ensureSuccess(response, uri);
    final payload = _decodeMap(response.body);
    return FundTrendResponse.fromJson(payload);
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

  Future<List<WatchlistItem>> fetchWatchlist({
    String quoteSource = "auto",
  }) async {
    final source = _normalizeQuoteSource(quoteSource);
    final uri = Uri.parse("$baseUrl/api/watchlist").replace(
      queryParameters: <String, String>{
        "quote_source": source,
      },
    );
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

  Future<void> setWatchlistSector({
    required String code,
    required String sector,
    String name = "",
  }) async {
    final uri = Uri.parse("$baseUrl/api/watchlist/sector");
    final response = await _client
        .post(
          uri,
          headers: _backendHeaders({"content-type": "application/json"}),
          body: jsonEncode(<String, dynamic>{
            "code": code.trim(),
            "sector": sector.trim(),
            "name": name.trim(),
          }),
        )
        .timeout(_requestTimeout);
    _ensureSuccess(response, uri);
  }

  Future<FundAnalysis> analyzeWatchFund({
    required String code,
    String name = "",
    String quoteSource = "auto",
    bool includeAi = true,
    Duration? timeout,
  }) async {
    final source = _normalizeQuoteSource(quoteSource);
    final uri = Uri.parse("$baseUrl/api/watchlist/analyze").replace(
      queryParameters: <String, String>{
        "code": code.trim(),
        "quote_source": source,
        "include_ai": includeAi ? "true" : "false",
        if (name.trim().isNotEmpty) "name": name.trim(),
      },
    );
    final response = await _client
        .get(uri, headers: _backendHeaders())
        .timeout(timeout ?? _requestTimeout);
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

  Future<Map<String, dynamic>> createInvestment({
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
    return _decodeMap(response.body);
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
