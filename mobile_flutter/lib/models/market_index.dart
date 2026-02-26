double _toDouble(dynamic value, [double fallback = 0]) {
  if (value == null) {
    return fallback;
  }
  if (value is num) {
    return value.toDouble();
  }
  return double.tryParse(value.toString()) ?? fallback;
}

class MarketIndexQuote {
  const MarketIndexQuote({
    required this.code,
    required this.name,
    required this.lastPrice,
    required this.changePct,
  });

  final String code;
  final String name;
  final double lastPrice;
  final double changePct;

  factory MarketIndexQuote.fromEastmoney(
    Map<String, dynamic> json, {
    required String fallbackName,
  }) {
    return MarketIndexQuote(
      code: (json["f12"] ?? "").toString(),
      name: fallbackName,
      lastPrice: _toDouble(json["f2"]),
      changePct: _toDouble(json["f3"]),
    );
  }
}
