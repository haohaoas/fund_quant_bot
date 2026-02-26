class WatchlistItem {
  const WatchlistItem({
    required this.id,
    required this.userId,
    required this.code,
    required this.name,
    required this.latestPrice,
    required this.latestPct,
    required this.sectorName,
    required this.sectorPct,
    required this.latestTime,
    required this.latestSource,
    required this.createdAt,
    required this.updatedAt,
  });

  final int id;
  final int userId;
  final String code;
  final String name;
  final double? latestPrice;
  final double? latestPct;
  final String sectorName;
  final double? sectorPct;
  final String latestTime;
  final String latestSource;
  final String createdAt;
  final String updatedAt;

  factory WatchlistItem.fromJson(Map<String, dynamic> json) {
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
      final text = value.toString().trim();
      return text.isEmpty ? fallback : text;
    }

    double? toDoubleOrNull(dynamic value) {
      if (value == null) {
        return null;
      }
      if (value is num) {
        return value.toDouble();
      }
      return double.tryParse(value.toString());
    }

    return WatchlistItem(
      id: toInt(json["id"]),
      userId: toInt(json["user_id"]),
      code: toText(json["code"]),
      name: toText(json["name"]),
      latestPrice: toDoubleOrNull(json["latest_price"]),
      latestPct: toDoubleOrNull(json["latest_pct"]),
      sectorName: toText(json["sector_name"]),
      sectorPct: toDoubleOrNull(json["sector_pct"]),
      latestTime: toText(json["latest_time"]),
      latestSource: toText(json["latest_source"]),
      createdAt: toText(json["created_at"]),
      updatedAt: toText(json["updated_at"]),
    );
  }

  String get displayName => name.isEmpty ? code : "$name ($code)";
}

class FundAnalysis {
  const FundAnalysis({
    required this.generatedAt,
    required this.code,
    required this.name,
    required this.latestPrice,
    required this.latestPct,
    required this.latestTime,
    required this.signalAction,
    required this.signalPositionHint,
    required this.signalReason,
    required this.basePrice,
    required this.grids,
    required this.sectorName,
    required this.sectorLevel,
    required this.sectorComment,
    required this.aiAction,
    required this.aiReason,
  });

  final String generatedAt;
  final String code;
  final String name;
  final double? latestPrice;
  final double? latestPct;
  final String latestTime;
  final String signalAction;
  final String signalPositionHint;
  final String signalReason;
  final double? basePrice;
  final List<double> grids;
  final String sectorName;
  final String sectorLevel;
  final String sectorComment;
  final String aiAction;
  final String aiReason;

  factory FundAnalysis.fromJson(Map<String, dynamic> json) {
    String toText(dynamic value, [String fallback = ""]) {
      if (value == null) {
        return fallback;
      }
      final text = value.toString().trim();
      return text.isEmpty ? fallback : text;
    }

    double? toDoubleOrNull(dynamic value) {
      if (value == null) {
        return null;
      }
      if (value is num) {
        return value.toDouble();
      }
      return double.tryParse(value.toString());
    }

    final latest = (json["latest"] as Map?)?.cast<String, dynamic>() ??
        const <String, dynamic>{};
    final signal = (json["signal"] as Map?)?.cast<String, dynamic>() ??
        const <String, dynamic>{};
    final sector = (json["sector"] as Map?)?.cast<String, dynamic>() ??
        const <String, dynamic>{};
    final ai = (json["ai_decision"] as Map?)?.cast<String, dynamic>() ??
        const <String, dynamic>{};

    final rawGrids = (signal["grids"] as List? ?? const [])
        .map(toDoubleOrNull)
        .whereType<double>()
        .toList();

    return FundAnalysis(
      generatedAt: toText(json["generated_at"]),
      code: toText(json["code"]),
      name: toText(json["name"]),
      latestPrice: toDoubleOrNull(latest["price"]),
      latestPct: toDoubleOrNull(latest["pct"]),
      latestTime: toText(latest["time"]),
      signalAction: toText(signal["action"], "HOLD"),
      signalPositionHint: toText(signal["position_hint"], "KEEP"),
      signalReason: toText(signal["reason"]),
      basePrice: toDoubleOrNull(signal["base_price"]),
      grids: rawGrids,
      sectorName: toText(sector["name"], "未知板块"),
      sectorLevel: toText(sector["level"], "中性"),
      sectorComment: toText(sector["comment"]),
      aiAction: toText(ai["action"], "HOLD"),
      aiReason: toText(ai["reason"]),
    );
  }
}
