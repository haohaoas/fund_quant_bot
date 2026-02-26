double _asDouble(dynamic value, [double fallback = 0]) {
  if (value == null) {
    return fallback;
  }
  if (value is num) {
    return value.toDouble();
  }
  return double.tryParse(value.toString()) ?? fallback;
}

String _asString(dynamic value, [String fallback = ""]) {
  if (value == null) {
    return fallback;
  }
  final text = value.toString();
  return text.isEmpty ? fallback : text;
}

class FundAction {
  const FundAction({
    required this.code,
    required this.name,
    required this.signal,
    required this.action,
    required this.reason,
    required this.latestPrice,
    required this.latestTime,
  });

  final String code;
  final String name;
  final String signal;
  final String action;
  final String reason;
  final double latestPrice;
  final String latestTime;

  factory FundAction.fromJson(Map<String, dynamic> json) {
    final latest = (json["latest"] as Map?)?.cast<String, dynamic>() ?? {};
    final decision = (json["ai_decision"] as Map?)?.cast<String, dynamic>() ?? {};
    return FundAction(
      code: _asString(json["code"], "-"),
      name: _asString(json["name"], "-"),
      signal: _asString(json["signal"], "HOLD"),
      action: _asString(decision["action"], "HOLD"),
      reason: _asString(decision["reason"], ""),
      latestPrice: _asDouble(latest["price"]),
      latestTime: _asString(latest["time"], ""),
    );
  }
}

class RecommendationSummary {
  const RecommendationSummary({
    required this.headline,
    required this.riskNotes,
  });

  final String headline;
  final List<String> riskNotes;

  factory RecommendationSummary.fromJson(Map<String, dynamic> json) {
    final notes = (json["risk_notes"] as List? ?? [])
        .map((item) => _asString(item))
        .where((item) => item.isNotEmpty)
        .toList();
    return RecommendationSummary(
      headline: _asString(json["headline"], ""),
      riskNotes: notes,
    );
  }
}

class MarketSummary {
  const MarketSummary({
    required this.sentiment,
    required this.score,
    required this.hotSectors,
    required this.suggestedStyle,
  });

  final String sentiment;
  final double score;
  final List<String> hotSectors;
  final String suggestedStyle;

  factory MarketSummary.fromJson(Map<String, dynamic> json) {
    final sectors = (json["hot_sectors"] as List? ?? [])
        .map((item) => _asString(item))
        .where((item) => item.isNotEmpty)
        .toList();
    return MarketSummary(
      sentiment: _asString(json["sentiment"], "neutral"),
      score: _asDouble(json["score"], 0),
      hotSectors: sectors,
      suggestedStyle: _asString(json["suggested_style"], ""),
    );
  }
}

class RecommendationsResponse {
  const RecommendationsResponse({
    required this.generatedAt,
    required this.cached,
    required this.computing,
    required this.cacheAgeSeconds,
    required this.summary,
    required this.actions,
    required this.market,
  });

  final String generatedAt;
  final bool cached;
  final bool computing;
  final int? cacheAgeSeconds;
  final RecommendationSummary summary;
  final List<FundAction> actions;
  final MarketSummary market;

  factory RecommendationsResponse.fromJson(Map<String, dynamic> json) {
    final summary = (json["summary"] as Map?)?.cast<String, dynamic>() ?? {};
    final market = (json["market"] as Map?)?.cast<String, dynamic>() ?? {};
    final actionsRaw = (json["actions"] as List? ?? [])
        .whereType<Map>()
        .map((item) => FundAction.fromJson(item.cast<String, dynamic>()))
        .toList();
    return RecommendationsResponse(
      generatedAt: _asString(json["generated_at"], ""),
      cached: json["cached"] == true,
      computing: json["computing"] == true,
      cacheAgeSeconds: json["cache_age_seconds"] is int
          ? json["cache_age_seconds"] as int
          : int.tryParse(_asString(json["cache_age_seconds"])),
      summary: RecommendationSummary.fromJson(summary),
      actions: actionsRaw,
      market: MarketSummary.fromJson(market),
    );
  }
}
