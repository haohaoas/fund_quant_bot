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

class PortfolioAccount {
  const PortfolioAccount({
    required this.cash,
    required this.totalMarketValue,
    required this.dailyProfit,
    required this.holdingProfit,
    required this.totalAsset,
  });

  final double cash;
  final double totalMarketValue;
  final double dailyProfit;
  final double holdingProfit;
  final double totalAsset;

  factory PortfolioAccount.fromJson(Map<String, dynamic> json) {
    return PortfolioAccount(
      cash: _toDouble(json["cash"]),
      totalMarketValue: _toDouble(json["total_market_value"]),
      dailyProfit: _toDouble(json["daily_profit"]),
      holdingProfit: _toDouble(json["holding_profit"]),
      totalAsset: _toDouble(json["total_asset"]),
    );
  }
}

class PortfolioPosition {
  const PortfolioPosition({
    required this.code,
    required this.name,
    required this.sector,
    required this.sectorPct,
    required this.shares,
    required this.cost,
    required this.latestNav,
    required this.navDate,
    required this.navSettled,
    required this.dailyChangePct,
    required this.dailyProfit,
    required this.marketValue,
    required this.holdingProfit,
    required this.holdingProfitPct,
  });

  final String code;
  final String name;
  final String sector;
  final double? sectorPct;
  final double shares;
  final double cost;
  final double? latestNav;
  final String navDate;
  final bool navSettled;
  final double? dailyChangePct;
  final double? dailyProfit;
  final double? marketValue;
  final double? holdingProfit;
  final double? holdingProfitPct;

  factory PortfolioPosition.fromJson(Map<String, dynamic> json) {
    double? parseNullableDouble(dynamic value) {
      if (value == null) {
        return null;
      }
      if (value is num) {
        return value.toDouble();
      }
      return double.tryParse(value.toString());
    }

    bool parseBool(dynamic value, [bool fallback = false]) {
      if (value == null) {
        return fallback;
      }
      if (value is bool) {
        return value;
      }
      if (value is num) {
        return value != 0;
      }
      final text = value.toString().trim().toLowerCase();
      if (text == "true" || text == "1" || text == "yes") {
        return true;
      }
      if (text == "false" || text == "0" || text == "no") {
        return false;
      }
      return fallback;
    }

    return PortfolioPosition(
      code: _toStringValue(json["code"], "-"),
      name: _toStringValue(json["name"], _toStringValue(json["code"], "-")),
      sector: _toStringValue(json["sector"]),
      sectorPct: parseNullableDouble(json["sector_pct"]),
      shares: _toDouble(json["shares"]),
      cost: _toDouble(json["cost"]),
      latestNav: parseNullableDouble(json["latest_nav"]),
      navDate: _toStringValue(json["jzrq"]),
      navSettled: parseBool(json["nav_settled"]),
      dailyChangePct: parseNullableDouble(json["daily_change_pct"]),
      dailyProfit: parseNullableDouble(json["daily_profit"]),
      marketValue: parseNullableDouble(json["market_value"]),
      holdingProfit: parseNullableDouble(json["holding_profit"]),
      holdingProfitPct: parseNullableDouble(json["holding_profit_pct"]),
    );
  }
}

class PortfolioResponse {
  const PortfolioResponse({
    required this.generatedAt,
    required this.cash,
    required this.account,
    required this.positions,
  });

  final String generatedAt;
  final double cash;
  final PortfolioAccount account;
  final List<PortfolioPosition> positions;

  factory PortfolioResponse.fromJson(Map<String, dynamic> json) {
    final accountJson =
        (json["account"] as Map?)?.cast<String, dynamic>() ?? {};
    final positions = (json["positions"] as List? ?? [])
        .whereType<Map>()
        .map((item) => PortfolioPosition.fromJson(item.cast<String, dynamic>()))
        .toList();

    return PortfolioResponse(
      generatedAt: _toStringValue(json["generated_at"]),
      cash: _toDouble(json["cash"]),
      account: PortfolioAccount.fromJson(accountJson),
      positions: positions,
    );
  }
}
