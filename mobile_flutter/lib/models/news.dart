String _asText(dynamic value, [String fallback = ""]) {
  if (value == null) {
    return fallback;
  }
  final text = value.toString();
  return text.isEmpty ? fallback : text;
}

class NewsItem {
  const NewsItem({
    required this.title,
    required this.summary,
    required this.url,
    required this.ctime,
    required this.mediaName,
  });

  final String title;
  final String summary;
  final String url;
  final String ctime;
  final String mediaName;

  factory NewsItem.fromJson(Map<String, dynamic> json) {
    return NewsItem(
      title: _asText(json["title"]),
      summary: _asText(json["summary"]),
      url: _asText(json["url"]),
      ctime: _asText(json["ctime"]),
      mediaName: _asText(json["media_name"]),
    );
  }
}

class NewsListResponse {
  const NewsListResponse({
    required this.ok,
    required this.generatedAt,
    required this.source,
    required this.items,
  });

  final bool ok;
  final String generatedAt;
  final String source;
  final List<NewsItem> items;

  factory NewsListResponse.fromJson(Map<String, dynamic> json) {
    final rows = (json["items"] as List? ?? [])
        .whereType<Map>()
        .map((item) => NewsItem.fromJson(item.cast<String, dynamic>()))
        .toList();
    return NewsListResponse(
      ok: json["ok"] == true,
      generatedAt: _asText(json["generated_at"]),
      source: _asText(json["source"], "-"),
      items: rows,
    );
  }
}
