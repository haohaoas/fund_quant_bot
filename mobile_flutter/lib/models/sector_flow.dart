double _toDouble(dynamic value, [double fallback = 0]) {
  if (value == null) {
    return fallback;
  }
  if (value is num) {
    return value.toDouble();
  }
  return double.tryParse(value.toString()) ?? fallback;
}

String _toStringValue(dynamic value, [String fallback = ""]) {
  if (value == null) {
    return fallback;
  }
  final text = value.toString();
  return text.isEmpty ? fallback : text;
}

class SectorItem {
  const SectorItem({
    required this.name,
    required this.mainNet,
    required this.mainInflow,
    required this.mainOutflow,
    required this.changePct,
    required this.unit,
  });

  final String name;
  final double mainNet;
  final double mainInflow;
  final double mainOutflow;
  final String changePct;
  final String unit;

  factory SectorItem.fromJson(Map<String, dynamic> json) {
    return SectorItem(
      name: _toStringValue(json["name"], "-"),
      mainNet: _toDouble(json["main_net"]),
      mainInflow: _toDouble(json["main_inflow"]),
      mainOutflow: _toDouble(json["main_outflow"]),
      changePct: _toStringValue(json["chg_pct"], "-"),
      unit: _toStringValue(json["unit"], ""),
    );
  }
}

class SectorFlowResponse {
  const SectorFlowResponse({
    required this.generatedAt,
    required this.fetchedAt,
    required this.indicator,
    required this.sectorType,
    required this.provider,
    required this.cached,
    required this.cacheAgeSeconds,
    required this.items,
  });

  final String generatedAt;
  final String fetchedAt;
  final String indicator;
  final String sectorType;
  final String provider;
  final bool cached;
  final int? cacheAgeSeconds;
  final List<SectorItem> items;

  factory SectorFlowResponse.fromJson(Map<String, dynamic> json) {
    final items = (json["items"] as List? ?? [])
        .whereType<Map>()
        .map((item) => SectorItem.fromJson(item.cast<String, dynamic>()))
        .toList();

    return SectorFlowResponse(
      generatedAt: _toStringValue(json["generated_at"]),
      fetchedAt: _toStringValue(json["fetched_at"]),
      indicator: _toStringValue(json["indicator"], "今日"),
      sectorType: _toStringValue(json["sector_type"], "行业资金流"),
      provider: _toStringValue(json["provider"], "auto"),
      cached: json["cached"] == true,
      cacheAgeSeconds: json["cache_age_seconds"] is int
          ? json["cache_age_seconds"] as int
          : int.tryParse(_toStringValue(json["cache_age_seconds"])),
      items: items,
    );
  }
}
