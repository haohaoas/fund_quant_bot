class AppAccount {
  const AppAccount({
    required this.id,
    required this.name,
    required this.avatar,
    required this.cash,
    required this.createdAt,
    required this.updatedAt,
  });

  final int id;
  final String name;
  final String avatar;
  final double cash;
  final String createdAt;
  final String updatedAt;

  factory AppAccount.fromJson(Map<String, dynamic> json) {
    double toDouble(dynamic value) {
      if (value == null) {
        return 0;
      }
      if (value is num) {
        return value.toDouble();
      }
      return double.tryParse(value.toString()) ?? 0;
    }

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

    return AppAccount(
      id: toInt(json["id"], 0),
      name: toText(json["name"], "账户"),
      avatar: toText(json["avatar"]),
      cash: toDouble(json["cash"]),
      createdAt: toText(json["created_at"]),
      updatedAt: toText(json["updated_at"]),
    );
  }
}
