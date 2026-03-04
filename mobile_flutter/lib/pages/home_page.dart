import "dart:async";
import "dart:convert";
import "dart:math" as math;

import "package:flutter/material.dart";
import "package:flutter/services.dart";
import "package:image_picker/image_picker.dart";
import "package:intl/intl.dart";
import "package:shared_preferences/shared_preferences.dart";

import "../models/account.dart";
import "../models/market_index.dart";
import "../models/news.dart";
import "../models/portfolio.dart";
import "../models/sector_flow.dart";
import "../models/watchlist.dart";
import "../services/api_client.dart";

class _ShortPullBouncingPhysics extends BouncingScrollPhysics {
  const _ShortPullBouncingPhysics({super.parent});

  @override
  _ShortPullBouncingPhysics applyTo(ScrollPhysics? ancestor) {
    return _ShortPullBouncingPhysics(parent: buildParent(ancestor));
  }

  @override
  double applyPhysicsToUserOffset(ScrollMetrics position, double offset) {
    final applied = super.applyPhysicsToUserOffset(position, offset);
    final overTop = position.pixels <= position.minScrollExtent && offset > 0;
    final overBottom =
        position.pixels >= position.maxScrollExtent && offset < 0;
    if (overTop || overBottom) {
      return applied * 0.7;
    }
    return applied;
  }
}

class _AddInvestmentDraft {
  const _AddInvestmentDraft({
    required this.code,
    required this.action,
    required this.amount,
    this.nav,
  });

  final String code;
  final String action;
  final double amount;
  final double? nav;
}

class _WatchlistDisplayItem {
  const _WatchlistDisplayItem({
    required this.code,
    required this.name,
    required this.latestPrice,
    required this.latestPct,
    required this.sectorName,
    required this.sectorPct,
    required this.isHolding,
    required this.canRemove,
  });

  final String code;
  final String name;
  final double? latestPrice;
  final double? latestPct;
  final String sectorName;
  final double? sectorPct;
  final bool isHolding;
  final bool canRemove;

  String get displayName => name.trim().isEmpty ? code : name.trim();
}

class HomePage extends StatefulWidget {
  const HomePage({
    super.key,
    required this.apiClient,
    required this.onLogout,
  });

  final ApiClient apiClient;
  final Future<void> Function() onLogout;

  @override
  State<HomePage> createState() => _HomePageState();
}

class _HomePageState extends State<HomePage> with WidgetsBindingObserver {
  static const List<String> _indicators = <String>["今日", "5日", "10日"];
  static const List<String> _sectorTypes = <String>["行业资金流", "概念资金流"];
  static const List<String> _actions = <String>["BUY", "SELL", "SIP", "REDEEM"];
  static const Duration _autoRefreshInterval = Duration(seconds: 5);
  static const String _quoteSourcePrefKey = "quote_source_mode";
  static const String _fundSearchHistoryPrefKey = "fund_search_history_v1";

  final NumberFormat _numberFormat = NumberFormat("#,##0.00", "zh_CN");

  int _tabIndex = 0;
  String _selectedIndicator = _indicators.first;
  String _selectedSectorType = _sectorTypes.first;

  PortfolioResponse? _portfolio;
  SectorFlowResponse? _sectorFlow;
  NewsListResponse? _newsList;
  List<WatchlistItem> _watchlist = const [];
  List<MarketIndexQuote> _majorIndices = const [];
  List<AppAccount> _accounts = const [];
  int? _selectedAccountId;

  String? _portfolioError;
  String? _watchlistError;
  String? _sectorError;
  String? _newsError;
  String? _indicesError;
  String? _accountsError;

  bool _loadingPortfolio = false;
  bool _loadingWatchlist = false;
  bool _loadingSector = false;
  bool _loadingNews = false;
  bool _loadingIndices = false;
  bool _loadingAccounts = false;
  bool _submittingInvestment = false;
  final Set<String> _deletingPositionCodes = <String>{};
  Timer? _autoRefreshTimer;
  bool _autoRefreshInFlight = false;
  String _quoteSourceMode = "estimate";

  @override
  void initState() {
    super.initState();
    WidgetsBinding.instance.addObserver(this);
    unawaited(_initPage());
    _startAutoRefresh();
  }

  Future<void> _initPage() async {
    await _restoreQuoteSourceMode();
    await _reload();
  }

  @override
  void dispose() {
    _stopAutoRefresh();
    WidgetsBinding.instance.removeObserver(this);
    super.dispose();
  }

  @override
  void didChangeAppLifecycleState(AppLifecycleState state) {
    if (state == AppLifecycleState.resumed) {
      _startAutoRefresh();
      unawaited(_resumePreRefresh());
      return;
    }
    if (state == AppLifecycleState.inactive ||
        state == AppLifecycleState.paused ||
        state == AppLifecycleState.detached) {
      _stopAutoRefresh();
    }
  }

  void _startAutoRefresh() {
    _autoRefreshTimer?.cancel();
    _autoRefreshTimer = Timer.periodic(_autoRefreshInterval, (_) {
      unawaited(_refreshCurrentTabSilently(forceRefresh: true));
    });
  }

  void _stopAutoRefresh() {
    _autoRefreshTimer?.cancel();
    _autoRefreshTimer = null;
  }

  Future<void> _resumePreRefresh() async {
    await _loadAccounts(showLoading: false);
    await _refreshCurrentTabSilently(forceRefresh: true);
  }

  Future<void> _refreshCurrentTabSilently({bool forceRefresh = false}) async {
    if (!mounted || _autoRefreshInFlight) {
      return;
    }
    if (_loadingPortfolio ||
        _loadingWatchlist ||
        _loadingSector ||
        _loadingNews ||
        _loadingIndices ||
        _loadingAccounts) {
      return;
    }
    _autoRefreshInFlight = true;
    try {
      if (_tabIndex == 0) {
        await _loadPortfolio(
          showLoading: false,
          forceRefresh: forceRefresh,
        );
        unawaited(_loadMajorIndices(showLoading: false));
      } else if (_tabIndex == 1) {
        await _loadWatchlist(showLoading: false);
      } else if (_tabIndex == 2) {
        await _loadSectorFlow(showLoading: false);
      } else if (_tabIndex == 3) {
        await _loadNews(showLoading: false);
      } else if (_tabIndex == 4) {
        await _loadAccounts(showLoading: false);
      } else {
        await _loadPortfolio(
          showLoading: false,
          forceRefresh: forceRefresh,
        );
      }
    } finally {
      _autoRefreshInFlight = false;
    }
  }

  Future<void> _reload({bool forceRefresh = false}) async {
    await _loadAccounts();
    unawaited(_loadWatchlist());
    unawaited(_loadSectorFlow());
    unawaited(_loadNews());
    unawaited(_loadMajorIndices());
    await _loadPortfolio(forceRefresh: forceRefresh);
  }

  int get _activeAccountId {
    if (_selectedAccountId != null) {
      return _selectedAccountId!;
    }
    if (_accounts.isNotEmpty) {
      return _accounts.first.id;
    }
    return 1;
  }

  int? get _resolvedAccountId {
    if (_selectedAccountId != null) {
      return _selectedAccountId;
    }
    if (_accounts.isNotEmpty) {
      return _accounts.first.id;
    }
    return null;
  }

  AppAccount? get _activeAccount {
    for (final account in _accounts) {
      if (account.id == _activeAccountId) {
        return account;
      }
    }
    return null;
  }

  ImageProvider<Object>? _avatarImageProvider(AppAccount? account) {
    final raw = (account?.avatar ?? "").trim();
    if (raw.isEmpty) {
      return null;
    }
    if (raw.startsWith("http://") || raw.startsWith("https://")) {
      return NetworkImage(raw);
    }
    const marker = "base64,";
    final markerIndex = raw.indexOf(marker);
    if (raw.startsWith("data:image") && markerIndex >= 0) {
      try {
        final base64Body = raw.substring(markerIndex + marker.length);
        return MemoryImage(base64Decode(base64Body));
      } catch (_) {
        return null;
      }
    }
    return null;
  }

  Future<void> _loadAccounts({bool showLoading = true}) async {
    if (showLoading) {
      setState(() {
        _loadingAccounts = true;
        _accountsError = null;
      });
    }

    try {
      final accounts = await widget.apiClient.fetchAccounts();
      if (!mounted) {
        return;
      }

      var selectedId = _selectedAccountId;
      if (accounts.isNotEmpty) {
        final containsSelected =
            selectedId != null && accounts.any((a) => a.id == selectedId);
        if (!containsSelected) {
          selectedId = accounts.first.id;
        }
      } else {
        selectedId = null;
      }

      setState(() {
        _accounts = accounts;
        _selectedAccountId = selectedId;
        _accountsError = null;
      });
    } catch (error) {
      if (!mounted) {
        return;
      }
      if (showLoading || _accounts.isEmpty) {
        setState(() {
          _accountsError = error.toString();
        });
      }
    } finally {
      if (mounted && showLoading) {
        setState(() {
          _loadingAccounts = false;
        });
      }
    }
  }

  Future<void> _loadPortfolio({
    bool forceRefresh = false,
    bool showLoading = true,
  }) async {
    if (showLoading) {
      setState(() {
        _loadingPortfolio = true;
        _portfolioError = null;
      });
    }

    try {
      final portfolio = await widget.apiClient.fetchPortfolio(
        forceRefresh: forceRefresh,
        accountId: _resolvedAccountId,
        quoteSource: _quoteSourceMode,
      );
      if (!mounted) {
        return;
      }
      setState(() {
        _portfolio = portfolio;
        _portfolioError = null;
      });
    } catch (error) {
      if (!mounted) {
        return;
      }
      if (showLoading || _portfolio == null) {
        setState(() {
          _portfolioError = error.toString();
        });
      }
    } finally {
      if (mounted && showLoading) {
        setState(() {
          _loadingPortfolio = false;
        });
      }
    }
  }

  Future<void> _loadWatchlist({bool showLoading = true}) async {
    if (showLoading) {
      setState(() {
        _loadingWatchlist = true;
        _watchlistError = null;
      });
    }

    try {
      final watchlist = await widget.apiClient.fetchWatchlist(
        quoteSource: _quoteSourceMode,
      );
      if (!mounted) {
        return;
      }
      setState(() {
        _watchlist = watchlist;
        _watchlistError = null;
      });
    } catch (error) {
      if (!mounted) {
        return;
      }
      if (showLoading || _watchlist.isEmpty) {
        setState(() {
          _watchlistError = error.toString();
        });
      }
    } finally {
      if (mounted && showLoading) {
        setState(() {
          _loadingWatchlist = false;
        });
      }
    }
  }

  Future<void> _loadSectorFlow({bool showLoading = true}) async {
    if (showLoading) {
      setState(() {
        _loadingSector = true;
        _sectorError = null;
      });
    }

    try {
      final flow = await widget.apiClient.fetchSectorFlow(
        indicator: _selectedIndicator,
        sectorType: _selectedSectorType,
        topN: 200,
      );
      if (!mounted) {
        return;
      }
      setState(() {
        _sectorFlow = flow;
        _sectorError = null;
      });
    } catch (error) {
      if (!mounted) {
        return;
      }
      if (showLoading || _sectorFlow == null) {
        setState(() {
          _sectorError = error.toString();
        });
      }
    } finally {
      if (mounted && showLoading) {
        setState(() {
          _loadingSector = false;
        });
      }
    }
  }

  Future<void> _loadMajorIndices({bool showLoading = true}) async {
    if (showLoading) {
      setState(() {
        _loadingIndices = true;
        _indicesError = null;
      });
    }

    try {
      final quotes = await widget.apiClient.fetchMajorIndices();
      if (!mounted) {
        return;
      }
      setState(() {
        _majorIndices = quotes;
        _indicesError = null;
      });
    } catch (error) {
      if (!mounted) {
        return;
      }
      if (showLoading || _majorIndices.isEmpty) {
        setState(() {
          _indicesError = error.toString();
        });
      }
    } finally {
      if (mounted && showLoading) {
        setState(() {
          _loadingIndices = false;
        });
      }
    }
  }

  Future<void> _loadNews({bool showLoading = true}) async {
    if (showLoading) {
      setState(() {
        _loadingNews = true;
        _newsError = null;
      });
    }

    try {
      final data = await widget.apiClient.fetchNewsList(limit: 60);
      if (!mounted) {
        return;
      }
      setState(() {
        _newsList = data;
        _newsError = null;
      });
    } catch (error) {
      if (!mounted) {
        return;
      }
      if (showLoading || _newsList == null) {
        setState(() {
          _newsError = error.toString();
        });
      }
    } finally {
      if (mounted && showLoading) {
        setState(() {
          _loadingNews = false;
        });
      }
    }
  }

  Future<List<FundSearchResult>> _loadFundSearchHistory() async {
    try {
      final prefs = await SharedPreferences.getInstance();
      final raw = prefs.getStringList(_fundSearchHistoryPrefKey) ?? const [];
      final out = <FundSearchResult>[];
      for (final item in raw) {
        final parts = item.split("|");
        if (parts.isEmpty) {
          continue;
        }
        final code = parts.first.trim();
        if (code.isEmpty) {
          continue;
        }
        final name = parts.length > 1 ? parts.sublist(1).join("|").trim() : "";
        out.add(FundSearchResult(code: code, name: name));
      }
      return out;
    } catch (_) {
      return const [];
    }
  }

  Future<void> _saveFundSearchHistory(FundSearchResult item) async {
    final code = item.code.trim();
    if (code.isEmpty) {
      return;
    }
    final encoded = "$code|${item.name.trim()}";
    final existing = await _loadFundSearchHistory();
    final merged = <String>[
      encoded,
      ...existing
          .where((e) => e.code.trim() != code)
          .map((e) => "${e.code.trim()}|${e.name.trim()}"),
    ].take(20).toList();

    try {
      final prefs = await SharedPreferences.getInstance();
      await prefs.setStringList(_fundSearchHistoryPrefKey, merged);
    } catch (_) {
      // ignore write failure
    }
  }

  Future<void> _clearFundSearchHistory() async {
    try {
      final prefs = await SharedPreferences.getInstance();
      await prefs.remove(_fundSearchHistoryPrefKey);
    } catch (_) {
      // ignore
    }
  }

  Future<FundSearchResult?> _openFundSearchPage({
    String initialKeyword = "",
  }) async {
    final history = await _loadFundSearchHistory();
    if (!mounted) {
      return null;
    }
    final picked = await Navigator.of(context).push<FundSearchResult>(
      MaterialPageRoute(
        builder: (_) => _FundSearchPage(
          apiClient: widget.apiClient,
          initialKeyword: initialKeyword,
          initialHistory: history,
          onClearHistory: _clearFundSearchHistory,
        ),
      ),
    );
    if (picked != null) {
      await _saveFundSearchHistory(picked);
    }
    return picked;
  }

  Future<_AddInvestmentDraft?> _showAddHoldingSheet() async {
    final quickFunds = _buildWatchlistDisplayItems();
    final codeController = TextEditingController();
    final amountController = TextEditingController();
    final navController = TextEditingController();

    String selectedAction = _actions.first;
    String selectedQuickCode = "";
    String selectedFundName = "";
    String? formError;
    const quickAmounts = <double>[100, 300, 500, 1000, 2000];

    final draft = await showModalBottomSheet<_AddInvestmentDraft>(
      context: context,
      isScrollControlled: true,
      builder: (sheetContext) {
        return StatefulBuilder(
          builder: (context, setSheetState) {
            String normalizeCode(String raw) {
              final text = raw.trim();
              final match = RegExp(r"(\d{6})").firstMatch(text);
              if (match != null) {
                return match.group(1) ?? text;
              }
              return text;
            }

            void applyQuickFund(_WatchlistDisplayItem fund) {
              setSheetState(() {
                selectedQuickCode = fund.code;
                selectedFundName = fund.name.trim();
                codeController.text = fund.code;
                formError = null;
              });
            }

            void submit() {
              final code = normalizeCode(codeController.text);
              final amountText = amountController.text.trim();
              final navText = navController.text.trim();

              final amount = double.tryParse(amountText);
              final nav = navText.isEmpty ? null : double.tryParse(navText);

              if (code.isEmpty) {
                setSheetState(() {
                  formError = "请输入基金代码";
                });
                return;
              }
              if (amount == null || amount <= 0) {
                setSheetState(() {
                  formError = "请输入有效金额（>0）";
                });
                return;
              }
              if (navText.isNotEmpty && (nav == null || nav <= 0)) {
                setSheetState(() {
                  formError = "净值格式不正确";
                });
                return;
              }

              Navigator.of(context).pop(
                _AddInvestmentDraft(
                  code: code,
                  action: selectedAction,
                  amount: amount,
                  nav: nav,
                ),
              );
            }

            return Padding(
              padding: EdgeInsets.fromLTRB(
                16,
                16,
                16,
                16 + MediaQuery.of(context).viewInsets.bottom,
              ),
              child: Column(
                mainAxisSize: MainAxisSize.min,
                crossAxisAlignment: CrossAxisAlignment.start,
                children: [
                  const Text(
                    "新增持仓",
                    style: TextStyle(fontSize: 16, fontWeight: FontWeight.w700),
                  ),
                  const SizedBox(height: 12),
                  if (quickFunds.isNotEmpty) ...[
                    const Text(
                      "快捷选择（自选/持有）",
                      style: TextStyle(
                        fontSize: 13,
                        color: Color(0xFF6C7387),
                        fontWeight: FontWeight.w600,
                      ),
                    ),
                    const SizedBox(height: 8),
                    SingleChildScrollView(
                      scrollDirection: Axis.horizontal,
                      child: Row(
                        children: [
                          for (final fund in quickFunds.take(12))
                            Padding(
                              padding: const EdgeInsets.only(right: 8),
                              child: ChoiceChip(
                                label: Text(
                                  fund.displayName,
                                  overflow: TextOverflow.ellipsis,
                                ),
                                selected: selectedQuickCode == fund.code,
                                onSelected: (_) => applyQuickFund(fund),
                              ),
                            ),
                        ],
                      ),
                    ),
                    const SizedBox(height: 10),
                  ],
                  TextField(
                    controller: codeController,
                    readOnly: true,
                    onTap: () async {
                      final picked = await _openFundSearchPage(
                        initialKeyword: codeController.text.trim(),
                      );
                      if (picked == null) {
                        return;
                      }
                      setSheetState(() {
                        selectedQuickCode = picked.code;
                        selectedFundName = picked.name;
                        codeController.text = picked.code;
                        formError = null;
                      });
                    },
                    decoration: InputDecoration(
                      labelText: "基金代码（点击选择）",
                      helperText: selectedFundName.isEmpty
                          ? "支持搜索历史，快速回填"
                          : selectedFundName,
                      suffixIcon: const Icon(Icons.search_rounded),
                      border: const OutlineInputBorder(),
                      isDense: true,
                    ),
                  ),
                  const SizedBox(height: 10),
                  DropdownButtonFormField<String>(
                    initialValue: selectedAction,
                    decoration: const InputDecoration(
                      labelText: "动作",
                      border: OutlineInputBorder(),
                      isDense: true,
                    ),
                    items: _actions
                        .map((value) =>
                            DropdownMenuItem(value: value, child: Text(value)))
                        .toList(),
                    onChanged: (value) {
                      if (value == null) {
                        return;
                      }
                      setSheetState(() {
                        selectedAction = value;
                      });
                    },
                  ),
                  const SizedBox(height: 10),
                  TextField(
                    controller: amountController,
                    keyboardType:
                        const TextInputType.numberWithOptions(decimal: true),
                    textInputAction: TextInputAction.next,
                    onChanged: (_) {
                      setSheetState(() {
                        formError = null;
                      });
                    },
                    decoration: const InputDecoration(
                      labelText: "金额（元）",
                      border: OutlineInputBorder(),
                      isDense: true,
                    ),
                  ),
                  const SizedBox(height: 8),
                  Wrap(
                    spacing: 8,
                    runSpacing: 6,
                    children: [
                      for (final value in quickAmounts)
                        ChoiceChip(
                          label: Text("¥${value.toStringAsFixed(0)}"),
                          selected: amountController.text.trim() ==
                              value.toStringAsFixed(0),
                          onSelected: (_) {
                            setSheetState(() {
                              amountController.text = value.toStringAsFixed(0);
                              formError = null;
                            });
                          },
                        ),
                    ],
                  ),
                  const SizedBox(height: 10),
                  TextField(
                    controller: navController,
                    keyboardType:
                        const TextInputType.numberWithOptions(decimal: true),
                    textInputAction: TextInputAction.done,
                    onChanged: (_) {
                      setSheetState(() {
                        formError = null;
                      });
                    },
                    decoration: const InputDecoration(
                      labelText: "净值（可选，留空按昨净值）",
                      border: OutlineInputBorder(),
                      isDense: true,
                    ),
                  ),
                  if (formError != null) ...[
                    const SizedBox(height: 8),
                    Text(
                      formError!,
                      style: TextStyle(color: Colors.red.shade700),
                    ),
                  ],
                  const SizedBox(height: 14),
                  Row(
                    children: [
                      Expanded(
                        child: OutlinedButton(
                          onPressed: () => Navigator.of(context).pop(),
                          child: const Text("取消"),
                        ),
                      ),
                      const SizedBox(width: 10),
                      Expanded(
                        child: FilledButton(
                          onPressed: submit,
                          child: const Text("确认新增"),
                        ),
                      ),
                    ],
                  ),
                ],
              ),
            );
          },
        );
      },
    );

    // Delay disposal to the next event loop tick so sheet subtree is fully torn down.
    await Future<void>.delayed(Duration.zero);
    codeController.dispose();
    amountController.dispose();
    navController.dispose();

    return draft;
  }

  Future<void> _onEditAvatarPressed() async {
    final account = _activeAccount;
    if (account == null) {
      return;
    }
    final picker = ImagePicker();
    final picked = await picker.pickImage(
      source: ImageSource.gallery,
      imageQuality: 72,
      maxWidth: 1080,
    );
    if (picked == null) {
      return;
    }

    final bytes = await picked.readAsBytes();
    if (bytes.isEmpty) {
      if (mounted) {
        ScaffoldMessenger.of(
          context,
        ).showSnackBar(const SnackBar(content: Text("读取图片失败")));
      }
      return;
    }
    final mime = _avatarMimeFromPath(picked.path);
    final encoded = "data:$mime;base64,${base64Encode(bytes)}";
    if (encoded.length > 1900000) {
      if (mounted) {
        ScaffoldMessenger.of(
          context,
        ).showSnackBar(const SnackBar(content: Text("图片太大，请选择更小的照片")));
      }
      return;
    }

    try {
      final updated = await widget.apiClient.updateAccount(
        accountId: account.id,
        avatar: encoded,
      );
      if (!mounted) {
        return;
      }
      await _loadAccounts();
      if (!mounted) {
        return;
      }
      setState(() {
        _selectedAccountId = updated.id;
      });
      ScaffoldMessenger.of(context).showSnackBar(
        const SnackBar(content: Text("头像已更新为照片")),
      );
    } catch (error) {
      if (!mounted) {
        return;
      }
      ScaffoldMessenger.of(context).showSnackBar(
        SnackBar(content: Text("更新头像失败: $error")),
      );
    }
  }

  String _avatarMimeFromPath(String path) {
    final lower = path.toLowerCase();
    if (lower.endsWith(".png")) {
      return "image/png";
    }
    if (lower.endsWith(".webp")) {
      return "image/webp";
    }
    if (lower.endsWith(".heic") || lower.endsWith(".heif")) {
      return "image/heic";
    }
    return "image/jpeg";
  }

  Widget _buildAccountAvatar(AppAccount? account, {double radius = 24}) {
    final imageProvider = _avatarImageProvider(account);
    return CircleAvatar(
      radius: radius,
      backgroundColor: const Color(0xFFE8EEF9),
      foregroundImage: imageProvider,
      child: imageProvider == null
          ? Icon(
              Icons.person_rounded,
              size: radius * 1.1,
              color: const Color(0xFF7B8599),
            )
          : null,
    );
  }

  void _showFeatureHint(String title) {
    if (!mounted) {
      return;
    }
    ScaffoldMessenger.of(
      context,
    ).showSnackBar(SnackBar(content: Text("$title 暂未开放")));
  }

  String _quoteSourceLabel(String mode) {
    switch (mode) {
      case "fund123":
        return "数据源1";
      case "estimate":
        return "数据源2";
      case "settled":
        return "数据源3";
      default:
        return "数据源2";
    }
  }

  Future<void> _restoreQuoteSourceMode() async {
    try {
      final prefs = await SharedPreferences.getInstance();
      final saved =
          (prefs.getString(_quoteSourcePrefKey) ?? "").trim().toLowerCase();
      final mode =
          (saved == "estimate" || saved == "settled" || saved == "fund123")
              ? saved
              : "estimate";
      if (!mounted) {
        return;
      }
      setState(() {
        _quoteSourceMode = mode;
      });
    } catch (_) {
      // ignore local read errors and keep default mode.
    }
  }

  Future<void> _persistQuoteSourceMode(String mode) async {
    try {
      final prefs = await SharedPreferences.getInstance();
      await prefs.setString(_quoteSourcePrefKey, mode);
    } catch (_) {
      // ignore local write errors
    }
  }

  Future<void> _onSwitchQuoteSourcePressed() async {
    final selected = await showModalBottomSheet<String>(
      context: context,
      builder: (sheetContext) {
        Widget option({
          required String mode,
          required String title,
          required String subtitle,
        }) {
          final active = _quoteSourceMode == mode;
          return ListTile(
            title: Text(title),
            subtitle: Text(subtitle),
            trailing: Icon(
              active ? Icons.check_circle : Icons.radio_button_unchecked,
              color: active ? const Color(0xFF2E5ED7) : const Color(0xFF99A1B2),
            ),
            onTap: () => Navigator.of(sheetContext).pop(mode),
          );
        }

        return SafeArea(
          child: Column(
            mainAxisSize: MainAxisSize.min,
            children: [
              const SizedBox(height: 8),
              const Text(
                "切换数据源",
                style: TextStyle(fontSize: 16, fontWeight: FontWeight.w700),
              ),
              const SizedBox(height: 6),
              option(
                mode: "fund123",
                title: "数据源1",
                subtitle: "Fund123 估值",
              ),
              option(
                mode: "estimate",
                title: "数据源2",
                subtitle: "fundgz 估值",
              ),
              option(
                mode: "settled",
                title: "数据源3",
                subtitle: "结算净值",
              ),
              const SizedBox(height: 8),
            ],
          ),
        );
      },
    );

    if (selected == null || selected == _quoteSourceMode) {
      return;
    }

    if (!mounted) {
      return;
    }
    setState(() {
      _quoteSourceMode = selected;
    });
    await _persistQuoteSourceMode(selected);
    if (!mounted) {
      return;
    }
    ScaffoldMessenger.of(
      context,
    ).showSnackBar(
        SnackBar(content: Text("已切换：${_quoteSourceLabel(selected)}")));
    await _reload(forceRefresh: true);
  }

  Future<void> _openAccountInfoPage() async {
    final account = _activeAccount;
    if (account == null) {
      ScaffoldMessenger.of(
        context,
      ).showSnackBar(const SnackBar(content: Text("暂无账户信息")));
      return;
    }

    final action = await Navigator.of(context).push<String>(
      MaterialPageRoute(
        builder: (_) => _AccountInfoPage(
          account: account,
          holdingCount: _portfolio?.positions.length ?? 0,
          cashText: _fmt(account.cash),
        ),
      ),
    );

    if (!mounted || action == null) {
      return;
    }
    if (action == "logout") {
      await _logoutCurrentAccount();
      return;
    }
    if (action == "edit_avatar") {
      await _onEditAvatarPressed();
      return;
    }
  }

  Future<void> _logoutCurrentAccount() async {
    await widget.onLogout();
  }

  Future<void> _showAppInfoDialog() async {
    await showDialog<void>(
      context: context,
      builder: (dialogContext) {
        return AlertDialog(
          title: const Text("应用信息"),
          content: Column(
            mainAxisSize: MainAxisSize.min,
            crossAxisAlignment: CrossAxisAlignment.start,
            children: [
              Text("API: ${widget.apiClient.baseUrl}"),
              const SizedBox(height: 6),
              Text("持仓数量: ${_portfolio?.positions.length ?? 0}"),
              const SizedBox(height: 6),
              Text("自选数量: ${_watchlist.length}"),
              const SizedBox(height: 6),
              Text("资金流条目: ${_sectorFlow?.items.length ?? 0}"),
              if (_portfolio?.generatedAt.isNotEmpty == true) ...[
                const SizedBox(height: 6),
                Text("最近更新: ${_portfolio!.generatedAt}"),
              ],
            ],
          ),
          actions: [
            FilledButton(
              onPressed: () => Navigator.of(dialogContext).pop(),
              child: const Text("知道了"),
            ),
          ],
        );
      },
    );
  }

  Future<void> _onAddHoldingPressed() async {
    final draft = await _showAddHoldingSheet();
    if (draft == null) {
      return;
    }

    setState(() {
      _submittingInvestment = true;
    });

    try {
      await widget.apiClient.createInvestment(
        code: draft.code,
        amount: draft.amount,
        action: draft.action,
        nav: draft.nav,
        accountId: _activeAccountId,
      );

      if (!mounted) {
        return;
      }

      ScaffoldMessenger.of(context).showSnackBar(
        const SnackBar(content: Text("新增持仓成功")),
      );

      await _loadPortfolio(forceRefresh: true);
    } catch (error) {
      if (!mounted) {
        return;
      }
      ScaffoldMessenger.of(context).showSnackBar(
        SnackBar(content: Text("新增持仓失败: $error")),
      );
    } finally {
      if (mounted) {
        setState(() {
          _submittingInvestment = false;
        });
      }
    }
  }

  Future<bool> _confirmAndDeleteHolding(PortfolioPosition position) async {
    final code = position.code.trim();
    final deleteKey = "$_activeAccountId:$code";
    if (code.isEmpty || _deletingPositionCodes.contains(deleteKey)) {
      return false;
    }

    final confirmed = await showDialog<bool>(
      context: context,
      builder: (dialogContext) {
        return AlertDialog(
          title: const Text("删除持仓"),
          content: Text("确定删除 ${position.name}（$code）吗？"),
          actions: [
            TextButton(
              onPressed: () => Navigator.of(dialogContext).pop(false),
              child: const Text("取消"),
            ),
            FilledButton(
              onPressed: () => Navigator.of(dialogContext).pop(true),
              style: FilledButton.styleFrom(
                backgroundColor: Colors.red.shade600,
              ),
              child: const Text("删除"),
            ),
          ],
        );
      },
    );

    if (confirmed != true) {
      return false;
    }

    setState(() {
      _deletingPositionCodes.add(deleteKey);
    });

    try {
      await widget.apiClient.deletePosition(
        code,
        accountId: _activeAccountId,
      );
      if (!mounted) {
        return false;
      }
      ScaffoldMessenger.of(context).showSnackBar(
        SnackBar(content: Text("已删除持仓: ${position.name}")),
      );
      await _loadPortfolio(forceRefresh: true);
    } catch (error) {
      if (mounted) {
        ScaffoldMessenger.of(context).showSnackBar(
          SnackBar(content: Text("删除失败: $error")),
        );
      }
    } finally {
      if (mounted) {
        setState(() {
          _deletingPositionCodes.remove(deleteKey);
        });
      }
    }

    // Keep item in place and let refreshed data source update the list.
    return false;
  }

  Future<void> _openHoldingTrend(PortfolioPosition position) async {
    final code = position.code.trim();
    if (code.isEmpty) {
      return;
    }
    final name = position.name.trim().isEmpty ? code : position.name.trim();
    await Navigator.of(context).push(
      MaterialPageRoute(
        builder: (_) => _FundTrendPage(
          apiClient: widget.apiClient,
          code: code,
          name: name,
          quoteSource: _quoteSourceMode,
          position: position,
          accountTotalAsset: _portfolio?.account.totalAsset,
        ),
      ),
    );
  }

  Color _signedColor(double value) {
    return value >= 0 ? Colors.red.shade700 : Colors.green.shade700;
  }

  String _fmt(double value) => _numberFormat.format(value);

  String _shortNavDateLabel(String raw) {
    final text = raw.trim();
    if (text.isEmpty) {
      return "";
    }
    try {
      final parsed = DateTime.parse(text);
      return DateFormat("MM-dd").format(parsed);
    } catch (_) {
      if (text.length >= 10) {
        return text.substring(5, 10);
      }
      return text;
    }
  }

  String _updatedBadgeText(String navDate) {
    final short = _shortNavDateLabel(navDate);
    if (short.isEmpty) {
      return "已更新";
    }
    return "已更新 $short";
  }

  double _settledAccountAsset(PortfolioAccount account) {
    return account.totalAsset - account.dailyProfit;
  }

  double? _settledPositionValue(PortfolioPosition position) {
    final marketValue = position.marketValue;
    if (marketValue == null) {
      return null;
    }
    // Intraday mode: market value is already frozen by backend.
    // Do not subtract estimated daily profit again, otherwise value drifts.
    if (!position.navSettled) {
      return marketValue;
    }
    final dailyProfit = position.dailyProfit;
    if (dailyProfit == null) {
      return marketValue;
    }
    return marketValue - dailyProfit;
  }

  Widget _buildErrorCard(String? error) {
    if (error == null) {
      return const SizedBox.shrink();
    }

    return Card(
      color: Colors.red.shade50,
      child: Padding(
        padding: const EdgeInsets.all(12),
        child: Text(
          "请求失败: $error",
          style: TextStyle(color: Colors.red.shade900),
        ),
      ),
    );
  }

  TextStyle _riseFallStyle(double value, {bool large = false}) {
    return TextStyle(
      color: value >= 0 ? const Color(0xFFDE4C54) : const Color(0xFF17A34A),
      fontWeight: FontWeight.w700,
      fontSize: large ? 20 : 14,
    );
  }

  String _fmtWatchPct(double? value) {
    if (value == null) {
      return "--";
    }
    return "${value >= 0 ? "+" : ""}${value.toStringAsFixed(2)}%";
  }

  TextStyle _watchPctStyle(double? value) {
    if (value == null) {
      return const TextStyle(
        color: Color(0xFF7B8599),
        fontWeight: FontWeight.w700,
        fontSize: 14,
      );
    }
    return _riseFallStyle(value);
  }

  double? _parsePctText(String text) {
    final normalized = text.trim().replaceAll("%", "");
    if (normalized.isEmpty || normalized == "-") {
      return null;
    }
    return double.tryParse(normalized);
  }

  String _normalizeSectorText(String text) {
    var value = text.trim();
    if (value.isEmpty) {
      return "";
    }
    const tokens = <String>["板块", "概念", "行业", "主题", "产业", "赛道", "指数"];
    for (final token in tokens) {
      value = value.replaceAll(token, "");
    }
    return value.trim();
  }

  double? _matchSectorPctFromLoadedFlow(String sectorName) {
    final flow = _sectorFlow;
    if (flow == null) {
      return null;
    }
    final key = sectorName.trim();
    if (key.isEmpty || key == "未知板块") {
      return null;
    }

    final keyNorm = _normalizeSectorText(key);
    double? bestValue;
    var bestScore = 0.0;

    for (final item in flow.items) {
      final candidate = item.name.trim();
      if (candidate.isEmpty) {
        continue;
      }
      final candidateNorm = _normalizeSectorText(candidate);
      final pct = _parsePctText(item.changePct);
      if (pct == null) {
        continue;
      }

      if (candidate == key ||
          candidateNorm == keyNorm ||
          candidate.contains(key) ||
          key.contains(candidate) ||
          (keyNorm.isNotEmpty &&
              candidateNorm.isNotEmpty &&
              (candidateNorm.contains(keyNorm) ||
                  keyNorm.contains(candidateNorm)))) {
        return pct;
      }

      final prefixHit = keyNorm.isNotEmpty &&
          candidateNorm.isNotEmpty &&
          keyNorm.length >= 2 &&
          candidateNorm.length >= 2 &&
          keyNorm.substring(0, 2) == candidateNorm.substring(0, 2);
      final score = prefixHit ? 0.6 : 0.0;
      if (score > bestScore) {
        bestScore = score;
        bestValue = pct;
      }
    }
    return bestValue;
  }

  List<_WatchlistDisplayItem> _buildWatchlistDisplayItems() {
    final merged = <String, _WatchlistDisplayItem>{};

    for (final item in _watchlist) {
      final code = item.code.trim();
      if (code.isEmpty) {
        continue;
      }
      merged[code] = _WatchlistDisplayItem(
        code: code,
        name: item.name.trim(),
        latestPrice: item.latestPrice,
        latestPct: item.latestPct,
        sectorName: item.sectorName.trim(),
        sectorPct:
            item.sectorPct ?? _matchSectorPctFromLoadedFlow(item.sectorName),
        isHolding: false,
        canRemove: true,
      );
    }

    final positions = _portfolio?.positions ?? const <PortfolioPosition>[];
    for (final position in positions) {
      final code = position.code.trim();
      if (code.isEmpty) {
        continue;
      }

      final current = merged[code];
      if (current == null) {
        merged[code] = _WatchlistDisplayItem(
          code: code,
          name: position.name.trim(),
          latestPrice: position.latestNav,
          latestPct: position.dailyChangePct,
          sectorName: position.sector.trim(),
          sectorPct: position.sectorPct ??
              _matchSectorPctFromLoadedFlow(position.sector),
          isHolding: true,
          canRemove: false,
        );
        continue;
      }

      merged[code] = _WatchlistDisplayItem(
        code: code,
        name: current.name.isEmpty ? position.name.trim() : current.name,
        latestPrice: current.latestPrice ?? position.latestNav,
        latestPct: current.latestPct ?? position.dailyChangePct,
        sectorName: current.sectorName.isEmpty
            ? position.sector.trim()
            : current.sectorName,
        sectorPct: current.sectorPct ??
            position.sectorPct ??
            _matchSectorPctFromLoadedFlow(
              current.sectorName.isEmpty ? position.sector : current.sectorName,
            ),
        isHolding: true,
        canRemove: current.canRemove,
      );
    }

    final items = merged.values.toList();
    items.sort((a, b) {
      if (a.isHolding != b.isHolding) {
        return a.isHolding ? -1 : 1;
      }
      return a.displayName.compareTo(b.displayName);
    });
    return items;
  }

  WatchlistItem? _findWatchlistSource(String code) {
    for (final item in _watchlist) {
      if (item.code.trim() == code) {
        return item;
      }
    }
    return null;
  }

  Widget _buildWatchlistTopBar({
    required int totalCount,
    required int holdingCount,
  }) {
    return Container(
      padding: const EdgeInsets.fromLTRB(12, 10, 12, 8),
      decoration: BoxDecoration(
        color: Colors.white,
        borderRadius: BorderRadius.circular(12),
      ),
      child: Column(
        children: [
          Row(
            children: [
              const Text(
                "自选",
                style: TextStyle(
                  fontSize: 20,
                  fontWeight: FontWeight.w700,
                  color: Color(0xFF1F2A44),
                ),
              ),
              const SizedBox(width: 8),
              Text(
                "$totalCount 支",
                style: const TextStyle(
                  fontSize: 12,
                  color: Color(0xFF7B8599),
                ),
              ),
              const Spacer(),
              FilledButton.tonalIcon(
                onPressed: _loadingWatchlist ? null : _onAddWatchlistPressed,
                icon: const Icon(Icons.add_rounded, size: 18),
                label: const Text("添加"),
                style: FilledButton.styleFrom(
                  visualDensity: VisualDensity.compact,
                  padding:
                      const EdgeInsets.symmetric(horizontal: 10, vertical: 8),
                ),
              ),
            ],
          ),
          const SizedBox(height: 8),
          Row(
            children: [
              Container(
                padding: const EdgeInsets.only(bottom: 4),
                decoration: const BoxDecoration(
                  border: Border(
                    bottom: BorderSide(color: Color(0xFF2E5ED7), width: 2),
                  ),
                ),
                child: const Text(
                  "全部",
                  style: TextStyle(
                    fontSize: 16,
                    fontWeight: FontWeight.w700,
                    color: Color(0xFF1F2A44),
                  ),
                ),
              ),
              const SizedBox(width: 16),
              Text(
                "持有 $holdingCount",
                style: const TextStyle(
                  fontSize: 14,
                  color: Color(0xFF7B8599),
                ),
              ),
              const Spacer(),
              const Text(
                "持有默认并入自选",
                style: TextStyle(
                  fontSize: 12,
                  color: Color(0xFF9AA1B2),
                ),
              ),
            ],
          ),
        ],
      ),
    );
  }

  Widget _buildHoldingsTopBar() {
    final holdingsCount = _portfolio?.positions.length ?? 0;
    final generatedAt = _portfolio?.generatedAt;
    final topInset = MediaQuery.of(context).padding.top;

    return Container(
      color: Colors.white,
      padding: EdgeInsets.fromLTRB(14, 12 + topInset, 14, 10),
      child: Column(
        crossAxisAlignment: CrossAxisAlignment.start,
        children: [
          Row(
            children: [
              const Text(
                "我的持有",
                style: TextStyle(
                  fontSize: 30 * 0.56,
                  fontWeight: FontWeight.w700,
                  color: Color(0xFF2E5ED7),
                ),
              ),
              const Spacer(),
              FilledButton.tonalIcon(
                onPressed: _submittingInvestment ? null : _onAddHoldingPressed,
                icon: _submittingInvestment
                    ? const SizedBox(
                        width: 14,
                        height: 14,
                        child: CircularProgressIndicator(strokeWidth: 2),
                      )
                    : const Icon(Icons.add, size: 18),
                label: Text(_submittingInvestment ? "提交中" : "新增持仓"),
              ),
            ],
          ),
          const SizedBox(height: 6),
          Row(
            children: [
              Text(
                "持仓 $holdingsCount 只",
                style: const TextStyle(color: Color(0xFF7B8599), fontSize: 13),
              ),
              const SizedBox(width: 14),
              if (generatedAt != null && generatedAt.isNotEmpty)
                Expanded(
                  child: Text(
                    "更新于 $generatedAt",
                    maxLines: 1,
                    overflow: TextOverflow.ellipsis,
                    style: const TextStyle(
                      color: Color(0xFF7B8599),
                      fontSize: 13,
                    ),
                  ),
                )
              else
                const Expanded(
                  child: Text(
                    "下拉可刷新最新数据",
                    maxLines: 1,
                    overflow: TextOverflow.ellipsis,
                    style: TextStyle(color: Color(0xFF7B8599), fontSize: 13),
                  ),
                ),
              if (_loadingPortfolio) ...[
                const SizedBox(width: 8),
                const SizedBox(
                  width: 14,
                  height: 14,
                  child: CircularProgressIndicator(strokeWidth: 2),
                ),
              ],
            ],
          ),
        ],
      ),
    );
  }

  Widget _buildHoldingsAssetStrip() {
    final account = _portfolio?.account;
    if (account == null) {
      return const SizedBox.shrink();
    }
    final settledAsset = _settledAccountAsset(account);
    return Container(
      color: Colors.white,
      padding: const EdgeInsets.fromLTRB(14, 14, 14, 14),
      child: Row(
        crossAxisAlignment: CrossAxisAlignment.start,
        children: [
          Expanded(
            child: Column(
              crossAxisAlignment: CrossAxisAlignment.start,
              children: [
                const Text(
                  "账户资产(不含当日预估)",
                  style: TextStyle(color: Color(0xFF7B8599), fontSize: 13),
                ),
                const SizedBox(height: 4),
                Text(
                  _fmt(settledAsset),
                  style: const TextStyle(
                    fontSize: 36 * 0.56,
                    fontWeight: FontWeight.w700,
                    color: Color(0xFF1B2B4A),
                  ),
                ),
              ],
            ),
          ),
          const SizedBox(width: 18),
          Column(
            crossAxisAlignment: CrossAxisAlignment.end,
            children: [
              const Text(
                "当日预估收益",
                style: TextStyle(color: Color(0xFF7B8599), fontSize: 13),
              ),
              const SizedBox(height: 4),
              Text(
                "${account.dailyProfit >= 0 ? "+" : ""}${_fmt(account.dailyProfit)}",
                style: _riseFallStyle(account.dailyProfit, large: true),
              ),
            ],
          ),
        ],
      ),
    );
  }

  Widget _buildNoticeStrip() {
    return Container(
      color: Colors.white,
      margin: const EdgeInsets.only(top: 1),
      padding: const EdgeInsets.symmetric(horizontal: 14, vertical: 10),
      child: const Row(
        children: [
          Text(
            "提示",
            style: TextStyle(
              color: Color(0xFF2E5ED7),
              fontWeight: FontWeight.w700,
              fontSize: 14,
            ),
          ),
          SizedBox(width: 10),
          Expanded(
            child: Text(
              "新增持仓请点右上角；下拉刷新，左滑可删除。",
              maxLines: 1,
              overflow: TextOverflow.ellipsis,
              style: TextStyle(color: Color(0xFF1E2A47), fontSize: 14),
            ),
          ),
        ],
      ),
    );
  }

  Widget _buildHoldingsHeaderRow() {
    return Container(
      color: Colors.white,
      margin: const EdgeInsets.only(top: 1),
      padding: const EdgeInsets.fromLTRB(14, 10, 14, 8),
      child: const Row(
        children: [
          Expanded(
            flex: 5,
            child: Text("基金/市值", style: TextStyle(color: Color(0xFF7B8599))),
          ),
          Expanded(
            flex: 2,
            child: Text("当日涨幅",
                textAlign: TextAlign.center,
                style: TextStyle(color: Color(0xFF7B8599))),
          ),
          Expanded(
            flex: 3,
            child: Text("当日收益",
                textAlign: TextAlign.right,
                style: TextStyle(color: Color(0xFF7B8599))),
          ),
          Expanded(
            flex: 3,
            child: Text("持有收益",
                textAlign: TextAlign.right,
                style: TextStyle(color: Color(0xFF7B8599))),
          ),
        ],
      ),
    );
  }

  Widget _buildHoldingsListRows() {
    final portfolio = _portfolio;
    if (portfolio == null) {
      return const SizedBox.shrink();
    }

    final positions = [...portfolio.positions]..sort((a, b) =>
        (_settledPositionValue(b) ?? 0)
            .compareTo(_settledPositionValue(a) ?? 0));
    if (positions.isEmpty) {
      return Container(
        color: Colors.white,
        padding: const EdgeInsets.all(20),
        child: const Text(
          "暂无持仓数据，点击上方 + 新增持仓。",
          style: TextStyle(color: Color(0xFF7B8599)),
        ),
      );
    }

    return Container(
      color: Colors.white,
      child: Column(
        children: [
          for (final position in positions)
            Dismissible(
              key: ValueKey("position-$_activeAccountId-${position.code}"),
              direction: _deletingPositionCodes
                      .contains("$_activeAccountId:${position.code}")
                  ? DismissDirection.none
                  : DismissDirection.endToStart,
              background: Container(
                alignment: Alignment.centerRight,
                padding: const EdgeInsets.only(right: 18),
                color: const Color(0xFFEE4D4D),
                child: const Row(
                  mainAxisSize: MainAxisSize.min,
                  children: [
                    Icon(Icons.delete_outline, color: Colors.white, size: 20),
                    SizedBox(width: 6),
                    Text(
                      "删除",
                      style: TextStyle(
                        color: Colors.white,
                        fontWeight: FontWeight.w700,
                      ),
                    ),
                  ],
                ),
              ),
              confirmDismiss: (_) => _confirmAndDeleteHolding(position),
              child: Material(
                color: Colors.white,
                child: InkWell(
                  onTap: () => unawaited(_openHoldingTrend(position)),
                  child: Container(
                    padding: const EdgeInsets.fromLTRB(14, 12, 14, 12),
                    decoration: const BoxDecoration(
                      border:
                          Border(bottom: BorderSide(color: Color(0xFFEDEFF5))),
                    ),
                    child: Row(
                      children: [
                        Expanded(
                          flex: 5,
                          child: Column(
                            crossAxisAlignment: CrossAxisAlignment.start,
                            children: [
                              Text(
                                position.name,
                                maxLines: 1,
                                overflow: TextOverflow.ellipsis,
                                style: const TextStyle(
                                  fontSize: 16,
                                  color: Color(0xFF1F2A44),
                                  fontWeight: FontWeight.w600,
                                ),
                              ),
                              const SizedBox(height: 4),
                              Row(
                                children: [
                                  Text(
                                    "¥ ${_settledPositionValue(position) == null ? "--" : _fmt(_settledPositionValue(position)!)}",
                                    style: const TextStyle(
                                      color: Color(0xFF7B8599),
                                      fontSize: 14,
                                    ),
                                  ),
                                  if (position.navSettled) ...[
                                    const SizedBox(width: 6),
                                    Container(
                                      padding: const EdgeInsets.symmetric(
                                        horizontal: 5,
                                        vertical: 1,
                                      ),
                                      decoration: BoxDecoration(
                                        color: const Color(0xFFEAF8EF),
                                        borderRadius: BorderRadius.circular(4),
                                      ),
                                      child: Text(
                                        _updatedBadgeText(position.navDate),
                                        style: const TextStyle(
                                          color: Color(0xFF17A34A),
                                          fontSize: 11,
                                          fontWeight: FontWeight.w600,
                                        ),
                                      ),
                                    ),
                                  ],
                                ],
                              ),
                            ],
                          ),
                        ),
                        Expanded(
                          flex: 2,
                          child: Text(
                            position.dailyChangePct == null
                                ? "--"
                                : "${position.dailyChangePct! >= 0 ? "+" : ""}${position.dailyChangePct!.toStringAsFixed(2)}%",
                            textAlign: TextAlign.center,
                            style: _riseFallStyle(position.dailyChangePct ?? 0),
                          ),
                        ),
                        Expanded(
                          flex: 3,
                          child: Text(
                            position.dailyProfit == null
                                ? "--"
                                : "${position.dailyProfit! >= 0 ? "+" : ""}${_fmt(position.dailyProfit!)}",
                            textAlign: TextAlign.right,
                            style: _riseFallStyle(position.dailyProfit ?? 0),
                          ),
                        ),
                        Expanded(
                          flex: 3,
                          child: Text(
                            position.holdingProfit == null
                                ? "--"
                                : "${position.holdingProfit! >= 0 ? "+" : ""}${_fmt(position.holdingProfit!)}",
                            textAlign: TextAlign.right,
                            style: _riseFallStyle(position.holdingProfit ?? 0),
                          ),
                        ),
                      ],
                    ),
                  ),
                ),
              ),
            ),
        ],
      ),
    );
  }

  Widget _buildMajorIndicesStrip() {
    final quotes = _majorIndices;
    const fallbackNames = <String>["上证指数", "深证成指", "创业板指", "沪深300"];

    final display = <MarketIndexQuote>[];
    for (var i = 0; i < fallbackNames.length; i++) {
      if (i < quotes.length) {
        display.add(quotes[i]);
      } else {
        display.add(
          MarketIndexQuote(
            code: "",
            name: fallbackNames[i],
            lastPrice: 0,
            changePct: 0,
          ),
        );
      }
    }

    return Container(
      color: Colors.white,
      padding: const EdgeInsets.fromLTRB(10, 8, 10, 10),
      child: Row(
        children: [
          for (final item in display)
            Expanded(
              child: SizedBox(
                height: 94,
                child: Container(
                  margin: const EdgeInsets.symmetric(horizontal: 4),
                  padding: const EdgeInsets.all(10),
                  decoration: BoxDecoration(
                    color: const Color(0xFFFFF1F1),
                    borderRadius: BorderRadius.circular(10),
                  ),
                  child: Column(
                    crossAxisAlignment: CrossAxisAlignment.start,
                    children: [
                      Text(
                        item.name,
                        maxLines: 1,
                        overflow: TextOverflow.ellipsis,
                        style: const TextStyle(
                            fontSize: 11, color: Color(0xFF6D7487)),
                      ),
                      const SizedBox(height: 6),
                      FittedBox(
                        fit: BoxFit.scaleDown,
                        alignment: Alignment.centerLeft,
                        child: Text(
                          item.code.isEmpty ? "--" : _fmt(item.lastPrice),
                          style: _riseFallStyle(item.changePct),
                        ),
                      ),
                      const SizedBox(height: 2),
                      Text(
                        item.code.isEmpty
                            ? (_loadingIndices
                                ? "加载中"
                                : (_indicesError == null ? "--" : "加载失败"))
                            : "${item.changePct >= 0 ? "+" : ""}${item.changePct.toStringAsFixed(2)}%",
                        style: const TextStyle(
                            color: Color(0xFFDE4C54),
                            fontWeight: FontWeight.w600),
                      ),
                    ],
                  ),
                ),
              ),
            ),
        ],
      ),
    );
  }

  Future<(String, String)?> _showAddWatchlistDialog() async {
    final codeController = TextEditingController();
    final nameController = TextEditingController();
    String? errorText;
    String selectedName = "";

    final result = await showDialog<(String, String)>(
      context: context,
      builder: (dialogContext) {
        return StatefulBuilder(
          builder: (context, setDialogState) {
            void submit() {
              final code = codeController.text.trim();
              final name = nameController.text.trim();
              if (code.isEmpty) {
                setDialogState(() {
                  errorText = "请输入基金代码";
                });
                return;
              }
              Navigator.of(dialogContext).pop((code, name));
            }

            return AlertDialog(
              title: const Text("添加自选基金"),
              content: Column(
                mainAxisSize: MainAxisSize.min,
                children: [
                  TextField(
                    controller: codeController,
                    readOnly: true,
                    onTap: () async {
                      final picked = await _openFundSearchPage(
                        initialKeyword: codeController.text.trim(),
                      );
                      if (picked == null) {
                        return;
                      }
                      setDialogState(() {
                        codeController.text = picked.code;
                        selectedName = picked.name;
                        if (nameController.text.trim().isEmpty &&
                            picked.name.trim().isNotEmpty) {
                          nameController.text = picked.name.trim();
                        }
                        errorText = null;
                      });
                    },
                    decoration: InputDecoration(
                      labelText: "基金代码（点击选择）",
                      helperText:
                          selectedName.isEmpty ? "支持搜索历史" : selectedName,
                      suffixIcon: const Icon(Icons.search_rounded),
                      border: const OutlineInputBorder(),
                      isDense: true,
                    ),
                  ),
                  const SizedBox(height: 10),
                  TextField(
                    controller: nameController,
                    decoration: const InputDecoration(
                      labelText: "基金名称（可选）",
                      border: OutlineInputBorder(),
                      isDense: true,
                    ),
                  ),
                  if (errorText != null) ...[
                    const SizedBox(height: 8),
                    Text(
                      errorText!,
                      style:
                          TextStyle(color: Colors.red.shade700, fontSize: 12),
                    ),
                  ],
                ],
              ),
              actions: [
                TextButton(
                  onPressed: () => Navigator.of(dialogContext).pop(),
                  child: const Text("取消"),
                ),
                FilledButton(
                  onPressed: submit,
                  child: const Text("添加"),
                ),
              ],
            );
          },
        );
      },
    );

    await Future<void>.delayed(Duration.zero);
    codeController.dispose();
    nameController.dispose();
    return result;
  }

  Future<void> _onAddWatchlistPressed() async {
    final result = await _showAddWatchlistDialog();
    if (result == null) {
      return;
    }
    final (code, name) = result;
    try {
      await widget.apiClient.upsertWatchlist(code: code, name: name);
      if (!mounted) {
        return;
      }
      await _loadWatchlist();
      if (!mounted) {
        return;
      }
      ScaffoldMessenger.of(context).showSnackBar(
        SnackBar(content: Text("已添加自选: $code")),
      );
    } catch (error) {
      if (!mounted) {
        return;
      }
      ScaffoldMessenger.of(context).showSnackBar(
        SnackBar(content: Text("添加失败: $error")),
      );
    }
  }

  Future<void> _onDeleteWatchlistPressed(WatchlistItem item) async {
    final confirmed = await showDialog<bool>(
      context: context,
      builder: (dialogContext) {
        return AlertDialog(
          title: const Text("移除自选"),
          content: Text("确定移除 ${item.displayName} 吗？"),
          actions: [
            TextButton(
              onPressed: () => Navigator.of(dialogContext).pop(false),
              child: const Text("取消"),
            ),
            FilledButton(
              onPressed: () => Navigator.of(dialogContext).pop(true),
              style: FilledButton.styleFrom(
                backgroundColor: Colors.red.shade600,
              ),
              child: const Text("移除"),
            ),
          ],
        );
      },
    );
    if (confirmed != true) {
      return;
    }

    try {
      await widget.apiClient.removeWatchlist(item.code);
      if (!mounted) {
        return;
      }
      await _loadWatchlist();
    } catch (error) {
      if (!mounted) {
        return;
      }
      ScaffoldMessenger.of(context).showSnackBar(
        SnackBar(content: Text("移除失败: $error")),
      );
    }
  }

  Future<String?> _showEditSectorDialog(_WatchlistDisplayItem item) async {
    final raw = item.sectorName.trim();
    final initial = raw == "未知板块" ? "" : raw;
    final controller = TextEditingController(text: initial);
    final result = await showDialog<String>(
      context: context,
      builder: (dialogContext) {
        return AlertDialog(
          title: const Text("手动设置板块"),
          content: Column(
            mainAxisSize: MainAxisSize.min,
            crossAxisAlignment: CrossAxisAlignment.start,
            children: [
              Text(
                item.displayName,
                style: const TextStyle(
                  fontSize: 13,
                  color: Color(0xFF6C7387),
                ),
              ),
              const SizedBox(height: 10),
              TextField(
                controller: controller,
                maxLength: 20,
                decoration: const InputDecoration(
                  labelText: "板块名称",
                  hintText: "如：半导体、AI应用",
                  border: OutlineInputBorder(),
                  isDense: true,
                ),
              ),
              const SizedBox(height: 6),
              const Text(
                "留空并保存 = 恢复自动识别",
                style: TextStyle(
                  fontSize: 12,
                  color: Color(0xFF9AA1B2),
                ),
              ),
            ],
          ),
          actions: [
            TextButton(
              onPressed: () => Navigator.of(dialogContext).pop(null),
              child: const Text("取消"),
            ),
            FilledButton(
              onPressed: () =>
                  Navigator.of(dialogContext).pop(controller.text.trim()),
              child: const Text("保存"),
            ),
          ],
        );
      },
    );
    await Future<void>.delayed(Duration.zero);
    controller.dispose();
    return result;
  }

  Future<void> _onEditSectorPressed(_WatchlistDisplayItem item) async {
    final sector = await _showEditSectorDialog(item);
    if (sector == null) {
      return;
    }
    try {
      await widget.apiClient.setWatchlistSector(
        code: item.code,
        sector: sector,
        name: item.name,
      );
      if (!mounted) {
        return;
      }
      await _loadWatchlist(showLoading: false);
      if (!mounted) {
        return;
      }
      ScaffoldMessenger.of(context).showSnackBar(
        SnackBar(
          content: Text(
            sector.isEmpty ? "已恢复自动板块识别" : "已设置板块：$sector",
          ),
        ),
      );
    } catch (error) {
      if (!mounted) {
        return;
      }
      ScaffoldMessenger.of(context).showSnackBar(
        SnackBar(content: Text("保存板块失败: $error")),
      );
    }
  }

  Future<void> _showWatchlistItemActions(_WatchlistDisplayItem item) async {
    final action = await showModalBottomSheet<String>(
      context: context,
      showDragHandle: true,
      builder: (sheetContext) {
        return SafeArea(
          child: Column(
            mainAxisSize: MainAxisSize.min,
            children: [
              ListTile(
                leading: const Icon(Icons.edit_outlined),
                title: const Text("编辑板块"),
                subtitle: Text(item.sectorName.trim().isEmpty
                    ? "当前：未知板块"
                    : "当前：${item.sectorName.trim()}"),
                onTap: () => Navigator.of(sheetContext).pop("sector"),
              ),
              if (item.canRemove)
                ListTile(
                  leading: const Icon(Icons.remove_circle_outline,
                      color: Colors.red),
                  title: const Text("移除自选"),
                  onTap: () => Navigator.of(sheetContext).pop("delete"),
                ),
            ],
          ),
        );
      },
    );
    if (!mounted || action == null) {
      return;
    }
    if (action == "sector") {
      await _onEditSectorPressed(item);
      return;
    }
    if (action == "delete") {
      final src = _findWatchlistSource(item.code);
      if (src != null) {
        await _onDeleteWatchlistPressed(src);
      }
    }
  }

  Widget _buildControls() {
    return Container(
      padding: const EdgeInsets.fromLTRB(14, 14, 14, 12),
      decoration: BoxDecoration(
        borderRadius: BorderRadius.circular(16),
        gradient: const LinearGradient(
          colors: [Color(0xFFF8FBFF), Color(0xFFF3F7FF)],
          begin: Alignment.topLeft,
          end: Alignment.bottomRight,
        ),
        border: Border.all(color: const Color(0xFFDDE7FF)),
      ),
      child: Column(
        crossAxisAlignment: CrossAxisAlignment.start,
        children: [
          const Row(
            children: [
              Icon(Icons.tune_rounded, size: 18, color: Color(0xFF2E5ED7)),
              SizedBox(width: 6),
              Text(
                "资金流筛选",
                style: TextStyle(
                  fontSize: 14,
                  fontWeight: FontWeight.w700,
                  color: Color(0xFF1F2A44),
                ),
              ),
            ],
          ),
          const SizedBox(height: 12),
          _buildFlowFilterGroup(
            label: "板块类型",
            options: _sectorTypes,
            selected: _selectedSectorType,
            onSelect: _loadingSector
                ? null
                : (value) {
                    setState(() {
                      _selectedSectorType = value;
                    });
                    unawaited(_loadSectorFlow());
                  },
          ),
          const SizedBox(height: 10),
          _buildFlowFilterGroup(
            label: "统计周期",
            options: _indicators,
            selected: _selectedIndicator,
            onSelect: _loadingSector
                ? null
                : (value) {
                    setState(() {
                      _selectedIndicator = value;
                    });
                    unawaited(_loadSectorFlow());
                  },
          ),
        ],
      ),
    );
  }

  Widget _buildFlowFilterGroup({
    required String label,
    required List<String> options,
    required String selected,
    required ValueChanged<String>? onSelect,
  }) {
    return Column(
      crossAxisAlignment: CrossAxisAlignment.start,
      children: [
        Text(
          label,
          style: const TextStyle(
            fontSize: 12,
            color: Color(0xFF6B7285),
            fontWeight: FontWeight.w600,
          ),
        ),
        const SizedBox(height: 6),
        Wrap(
          spacing: 8,
          runSpacing: 8,
          children: options.map((item) {
            final active = item == selected;
            return InkWell(
              borderRadius: BorderRadius.circular(999),
              onTap: onSelect == null ? null : () => onSelect(item),
              child: AnimatedContainer(
                duration: const Duration(milliseconds: 160),
                padding:
                    const EdgeInsets.symmetric(horizontal: 12, vertical: 7),
                decoration: BoxDecoration(
                  borderRadius: BorderRadius.circular(999),
                  color: active
                      ? const Color(0xFF2E5ED7)
                      : const Color(0xFFFFFFFF),
                  border: Border.all(
                    color: active
                        ? const Color(0xFF2E5ED7)
                        : const Color(0xFFD7DEEE),
                  ),
                ),
                child: Text(
                  item,
                  style: TextStyle(
                    fontSize: 12,
                    fontWeight: FontWeight.w700,
                    color: active
                        ? const Color(0xFFFFFFFF)
                        : const Color(0xFF44506A),
                  ),
                ),
              ),
            );
          }).toList(),
        ),
      ],
    );
  }

  Widget _buildSectors(BuildContext context) {
    final flow = _sectorFlow;
    if (flow == null) {
      return const SizedBox.shrink();
    }

    final items = flow.items;
    final topCount = items.length;
    final netSum = items.fold<double>(0, (sum, item) => sum + item.mainNet);
    final risingCount =
        items.where((item) => (_parsePctText(item.changePct) ?? 0) >= 0).length;
    final ratioText = "$risingCount/$topCount";

    return Column(
      crossAxisAlignment: CrossAxisAlignment.start,
      children: [
        Container(
          width: double.infinity,
          padding: const EdgeInsets.fromLTRB(14, 12, 14, 12),
          decoration: BoxDecoration(
            borderRadius: BorderRadius.circular(16),
            color: const Color(0xFF1F2A44),
          ),
          child: Wrap(
            spacing: 16,
            runSpacing: 10,
            children: [
              _buildFlowStat("样本数量", "$topCount"),
              _buildFlowStat("上涨板块", ratioText),
              _buildFlowStat(
                "主力净额",
                "${netSum >= 0 ? "+" : ""}${_fmt(netSum)} ${items.isEmpty ? "" : items.first.unit}",
                valueColor: netSum >= 0
                    ? const Color(0xFFFFA9B0)
                    : const Color(0xFF8DE0A4),
              ),
            ],
          ),
        ),
        const SizedBox(height: 10),
        for (var i = 0; i < items.length; i++) ...[
          _buildSectorFlowCard(items[i], i + 1),
          if (i != items.length - 1) const SizedBox(height: 10),
        ],
      ],
    );
  }

  Widget _buildFlowStat(String label, String value, {Color? valueColor}) {
    return Column(
      crossAxisAlignment: CrossAxisAlignment.start,
      children: [
        Text(
          label,
          style: const TextStyle(
            fontSize: 11,
            color: Color(0xFFA8B3CB),
            fontWeight: FontWeight.w600,
          ),
        ),
        const SizedBox(height: 4),
        Text(
          value,
          style: TextStyle(
            fontSize: 15,
            fontWeight: FontWeight.w800,
            color: valueColor ?? const Color(0xFFFFFFFF),
          ),
        ),
      ],
    );
  }

  Widget _buildSectorFlowCard(SectorItem item, int rank) {
    final change = _parsePctText(item.changePct);
    final changeText = change == null
        ? item.changePct
        : "${change >= 0 ? "+" : ""}${change.toStringAsFixed(2)}%";
    final mainNetText =
        "${item.mainNet >= 0 ? "+" : ""}${_fmt(item.mainNet)} ${item.unit}";
    final totalFlow = item.mainInflow.abs() + item.mainOutflow.abs();
    final inflowRatio =
        totalFlow <= 0 ? 0.5 : (item.mainInflow.abs() / totalFlow);
    final inflowFlex = (inflowRatio * 1000).round().clamp(1, 999);
    final outflowFlex = (1000 - inflowFlex).clamp(1, 999);

    final gradient = item.mainNet >= 0
        ? const [Color(0xFFFFF6F7), Color(0xFFFFFFFF)]
        : const [Color(0xFFF4FBF6), Color(0xFFFFFFFF)];

    return Container(
      padding: const EdgeInsets.fromLTRB(12, 12, 12, 10),
      decoration: BoxDecoration(
        borderRadius: BorderRadius.circular(14),
        gradient: LinearGradient(
          colors: gradient,
          begin: Alignment.topLeft,
          end: Alignment.bottomRight,
        ),
        border: Border.all(color: const Color(0xFFE5EAF5)),
      ),
      child: Column(
        crossAxisAlignment: CrossAxisAlignment.start,
        children: [
          Row(
            children: [
              Container(
                width: 24,
                height: 24,
                alignment: Alignment.center,
                decoration: BoxDecoration(
                  color: rank <= 3
                      ? const Color(0xFF2E5ED7)
                      : const Color(0xFFE9EEFA),
                  borderRadius: BorderRadius.circular(999),
                ),
                child: Text(
                  "$rank",
                  style: TextStyle(
                    fontSize: 11,
                    fontWeight: FontWeight.w800,
                    color: rank <= 3
                        ? const Color(0xFFFFFFFF)
                        : const Color(0xFF5E6A84),
                  ),
                ),
              ),
              const SizedBox(width: 8),
              Expanded(
                child: Text(
                  item.name,
                  maxLines: 1,
                  overflow: TextOverflow.ellipsis,
                  style: const TextStyle(
                    fontSize: 15,
                    fontWeight: FontWeight.w700,
                    color: Color(0xFF1F2A44),
                  ),
                ),
              ),
              Container(
                padding: const EdgeInsets.symmetric(horizontal: 9, vertical: 4),
                decoration: BoxDecoration(
                  color: (change ?? 0) >= 0
                      ? const Color(0xFFFFEBED)
                      : const Color(0xFFE9F8EF),
                  borderRadius: BorderRadius.circular(999),
                ),
                child: Text(
                  changeText,
                  style: TextStyle(
                    fontSize: 12,
                    fontWeight: FontWeight.w700,
                    color: (change ?? 0) >= 0
                        ? const Color(0xFFDE4C54)
                        : const Color(0xFF17A34A),
                  ),
                ),
              ),
            ],
          ),
          const SizedBox(height: 10),
          Row(
            children: [
              const Text(
                "主力净额",
                style: TextStyle(
                  fontSize: 12,
                  color: Color(0xFF7B8599),
                  fontWeight: FontWeight.w600,
                ),
              ),
              const SizedBox(width: 8),
              Text(
                mainNetText,
                style: TextStyle(
                  fontSize: 16,
                  fontWeight: FontWeight.w800,
                  color: _signedColor(item.mainNet),
                ),
              ),
            ],
          ),
          const SizedBox(height: 9),
          ClipRRect(
            borderRadius: BorderRadius.circular(999),
            child: SizedBox(
              height: 7,
              child: Row(
                children: [
                  Expanded(
                    flex: inflowFlex,
                    child: Container(color: const Color(0xFF3F77F1)),
                  ),
                  Expanded(
                    flex: outflowFlex,
                    child: Container(color: const Color(0xFFF59E0B)),
                  ),
                ],
              ),
            ),
          ),
          const SizedBox(height: 8),
          Row(
            children: [
              Expanded(
                child: Text(
                  "流入 ${_fmt(item.mainInflow)} ${item.unit}",
                  style: const TextStyle(
                    fontSize: 12,
                    color: Color(0xFF4D5A75),
                    fontWeight: FontWeight.w600,
                  ),
                ),
              ),
              Text(
                "流出 ${_fmt(item.mainOutflow.abs())} ${item.unit}",
                style: const TextStyle(
                  fontSize: 12,
                  color: Color(0xFF7B8599),
                  fontWeight: FontWeight.w600,
                ),
              ),
            ],
          ),
        ],
      ),
    );
  }

  Widget _buildMeProfileCard() {
    final active = _activeAccount;
    return Material(
      color: Colors.white,
      borderRadius: BorderRadius.circular(16),
      child: InkWell(
        borderRadius: BorderRadius.circular(16),
        onTap:
            _loadingAccounts ? null : () => unawaited(_openAccountInfoPage()),
        child: Padding(
          padding: const EdgeInsets.fromLTRB(14, 14, 10, 14),
          child: Row(
            children: [
              _buildAccountAvatar(active, radius: 30),
              const SizedBox(width: 12),
              Expanded(
                child: Column(
                  crossAxisAlignment: CrossAxisAlignment.start,
                  children: [
                    Text(
                      active?.name ?? "默认账户",
                      maxLines: 1,
                      overflow: TextOverflow.ellipsis,
                      style: const TextStyle(
                        fontSize: 18,
                        fontWeight: FontWeight.w700,
                        color: Color(0xFF1F2A44),
                      ),
                    ),
                    const SizedBox(height: 4),
                    const Text(
                      "点击查看账户信息",
                      style: TextStyle(
                        color: Color(0xFF6F7891),
                        fontSize: 14,
                      ),
                    ),
                  ],
                ),
              ),
              const Icon(Icons.chevron_right_rounded, color: Color(0xFFB0B7C6)),
            ],
          ),
        ),
      ),
    );
  }

  Widget _buildMeSection(List<Widget> children) {
    return Container(
      decoration: BoxDecoration(
        color: Colors.white,
        borderRadius: BorderRadius.circular(16),
      ),
      child: Column(children: children),
    );
  }

  Widget _buildMeRow({
    required IconData icon,
    required String title,
    String? value,
    VoidCallback? onTap,
    bool showDivider = true,
    bool showChevron = true,
  }) {
    Widget content = Container(
      decoration: BoxDecoration(
        border: showDivider
            ? const Border(
                bottom: BorderSide(color: Color(0xFFF0F2F7), width: 1),
              )
            : null,
      ),
      padding: const EdgeInsets.fromLTRB(14, 13, 12, 13),
      child: Row(
        children: [
          Icon(icon, size: 23, color: const Color(0xFF6C7387)),
          const SizedBox(width: 10),
          Expanded(
            child: Text(
              title,
              style: const TextStyle(
                fontSize: 16,
                color: Color(0xFF1F2A44),
              ),
            ),
          ),
          if (value != null) ...[
            Flexible(
              child: Text(
                value,
                maxLines: 1,
                overflow: TextOverflow.ellipsis,
                style: const TextStyle(
                  fontSize: 14,
                  color: Color(0xFF7B8599),
                ),
              ),
            ),
            const SizedBox(width: 6),
          ],
          if (showChevron)
            const Icon(Icons.chevron_right_rounded, color: Color(0xFFB0B7C6)),
        ],
      ),
    );

    if (onTap == null) {
      return content;
    }
    return Material(
      color: Colors.transparent,
      child: InkWell(onTap: onTap, child: content),
    );
  }

  Widget _buildLoadingBody() {
    return ListView(
      physics: const AlwaysScrollableScrollPhysics(
        parent: ClampingScrollPhysics(),
      ),
      children: const [
        SizedBox(height: 120),
        Center(child: CircularProgressIndicator()),
      ],
    );
  }

  Widget _buildHoldingsTab(BuildContext context) {
    if (_loadingPortfolio && _portfolio == null) {
      return _buildLoadingBody();
    }

    return Column(
      children: [
        _buildHoldingsTopBar(),
        _buildErrorCard(_portfolioError),
        _buildHoldingsAssetStrip(),
        _buildNoticeStrip(),
        _buildHoldingsHeaderRow(),
        Expanded(
          child: RefreshIndicator(
            displacement: 28,
            onRefresh: () => _reload(forceRefresh: true),
            child: ListView(
              physics: const _ShortPullBouncingPhysics(
                parent: AlwaysScrollableScrollPhysics(),
              ),
              padding: EdgeInsets.zero,
              children: [
                _buildHoldingsListRows(),
                const SizedBox(height: 110),
              ],
            ),
          ),
        ),
      ],
    );
  }

  Widget _buildRecommendationsTab(BuildContext context) {
    final displayItems = _buildWatchlistDisplayItems();
    final holdingCount = _portfolio?.positions.length ?? 0;
    if (_loadingWatchlist && displayItems.isEmpty) {
      return _buildLoadingBody();
    }

    return ListView(
      physics: const AlwaysScrollableScrollPhysics(
        parent: ClampingScrollPhysics(),
      ),
      padding: const EdgeInsets.fromLTRB(10, 10, 10, 14),
      children: [
        _buildErrorCard(_watchlistError),
        _buildWatchlistTopBar(
          totalCount: displayItems.length,
          holdingCount: holdingCount,
        ),
        const SizedBox(height: 10),
        if (displayItems.isEmpty)
          Container(
            padding: const EdgeInsets.all(18),
            decoration: BoxDecoration(
              color: Colors.white,
              borderRadius: BorderRadius.circular(12),
            ),
            child: const Text("还没有基金，点击上方添加自选。"),
          )
        else
          Container(
            decoration: BoxDecoration(
              color: Colors.white,
              borderRadius: BorderRadius.circular(12),
            ),
            child: Column(
              children: [
                const Padding(
                  padding: EdgeInsets.fromLTRB(12, 10, 12, 8),
                  child: Row(
                    children: [
                      Expanded(
                        flex: 5,
                        child: Text(
                          "基金名称",
                          style: TextStyle(
                            color: Color(0xFF7B8599),
                            fontSize: 13,
                          ),
                        ),
                      ),
                      Expanded(
                        flex: 2,
                        child: Text(
                          "当日涨幅",
                          textAlign: TextAlign.right,
                          style: TextStyle(
                            color: Color(0xFF7B8599),
                            fontSize: 13,
                          ),
                        ),
                      ),
                      Expanded(
                        flex: 2,
                        child: Text(
                          "板块涨幅",
                          textAlign: TextAlign.right,
                          style: TextStyle(
                            color: Color(0xFF7B8599),
                            fontSize: 13,
                          ),
                        ),
                      ),
                    ],
                  ),
                ),
                for (final item in displayItems)
                  InkWell(
                    onTap: () {
                      final analysisName =
                          item.name.trim().isEmpty ? item.code : item.name;
                      Navigator.of(context).push(
                        MaterialPageRoute(
                          builder: (_) => _FundAnalysisPage(
                            apiClient: widget.apiClient,
                            code: item.code,
                            name: analysisName,
                            quoteSource: _quoteSourceMode,
                          ),
                        ),
                      );
                    },
                    onLongPress: () {
                      unawaited(_showWatchlistItemActions(item));
                    },
                    child: Container(
                      padding: const EdgeInsets.fromLTRB(12, 9, 12, 9),
                      decoration: const BoxDecoration(
                        border: Border(
                            bottom: BorderSide(color: Color(0xFFF0F2F7))),
                      ),
                      child: Row(
                        children: [
                          Expanded(
                            flex: 5,
                            child: Column(
                              crossAxisAlignment: CrossAxisAlignment.start,
                              children: [
                                Text(
                                  item.displayName,
                                  maxLines: 1,
                                  overflow: TextOverflow.ellipsis,
                                  style: const TextStyle(
                                    fontSize: 17,
                                    fontWeight: FontWeight.w600,
                                    color: Color(0xFF1F2A44),
                                  ),
                                ),
                                const SizedBox(height: 2),
                                Row(
                                  children: [
                                    Text(
                                      item.code,
                                      style: const TextStyle(
                                        fontSize: 13,
                                        color: Color(0xFF7B8599),
                                      ),
                                    ),
                                    if (item.isHolding) ...[
                                      const SizedBox(width: 6),
                                      Container(
                                        padding: const EdgeInsets.symmetric(
                                          horizontal: 5,
                                          vertical: 1,
                                        ),
                                        decoration: BoxDecoration(
                                          color: const Color(0xFFE8F0FF),
                                          borderRadius:
                                              BorderRadius.circular(4),
                                        ),
                                        child: const Text(
                                          "持有",
                                          style: TextStyle(
                                            fontSize: 11,
                                            color: Color(0xFF2E5ED7),
                                            fontWeight: FontWeight.w600,
                                          ),
                                        ),
                                      ),
                                    ],
                                    if (item.latestPrice != null) ...[
                                      const SizedBox(width: 8),
                                      Text(
                                        _fmt(item.latestPrice!),
                                        style: const TextStyle(
                                          fontSize: 12,
                                          color: Color(0xFF9AA1B2),
                                        ),
                                      ),
                                    ],
                                  ],
                                ),
                              ],
                            ),
                          ),
                          Expanded(
                            flex: 2,
                            child: Text(
                              _fmtWatchPct(item.latestPct),
                              textAlign: TextAlign.right,
                              style: _watchPctStyle(item.latestPct),
                            ),
                          ),
                          Expanded(
                            flex: 2,
                            child: Column(
                              crossAxisAlignment: CrossAxisAlignment.end,
                              children: [
                                Text(
                                  _fmtWatchPct(item.sectorPct),
                                  textAlign: TextAlign.right,
                                  style: _watchPctStyle(item.sectorPct),
                                ),
                                if (item.sectorName.isNotEmpty)
                                  Text(
                                    item.sectorName,
                                    maxLines: 1,
                                    overflow: TextOverflow.ellipsis,
                                    style: const TextStyle(
                                      fontSize: 12,
                                      color: Color(0xFF9AA1B2),
                                    ),
                                  ),
                              ],
                            ),
                          ),
                        ],
                      ),
                    ),
                  ),
              ],
            ),
          ),
        const SizedBox(height: 12),
      ],
    );
  }

  Widget _buildFlowTab(BuildContext context) {
    if (_loadingSector && _sectorFlow == null) {
      return _buildLoadingBody();
    }

    return ListView(
      physics: const AlwaysScrollableScrollPhysics(
        parent: ClampingScrollPhysics(),
      ),
      padding: const EdgeInsets.all(12),
      children: [
        _buildErrorCard(_sectorError),
        _buildControls(),
        const SizedBox(height: 10),
        _buildSectors(context),
        if (_sectorFlow != null)
          Container(
            margin: const EdgeInsets.only(top: 10),
            padding: const EdgeInsets.symmetric(horizontal: 10, vertical: 8),
            decoration: BoxDecoration(
              color: const Color(0xFFF5F7FC),
              borderRadius: BorderRadius.circular(10),
              border: Border.all(color: const Color(0xFFE3E9F6)),
            ),
            child: Row(
              children: [
                const Icon(
                  Icons.info_outline_rounded,
                  size: 14,
                  color: Color(0xFF7B8599),
                ),
                const SizedBox(width: 6),
                Expanded(
                  child: Text(
                    "源: ${_sectorFlow!.provider} | 周期: $_selectedIndicator | 类型: $_selectedSectorType",
                    style: const TextStyle(
                      fontSize: 11,
                      color: Color(0xFF6C7387),
                      fontWeight: FontWeight.w600,
                    ),
                  ),
                ),
              ],
            ),
          ),
        const SizedBox(height: 12),
      ],
    );
  }

  Widget _buildNewsTab(BuildContext context) {
    if (_loadingNews && _newsList == null) {
      return _buildLoadingBody();
    }

    final data = _newsList;
    final items = data?.items ?? const <NewsItem>[];

    return ListView(
      physics: const AlwaysScrollableScrollPhysics(
        parent: ClampingScrollPhysics(),
      ),
      padding: const EdgeInsets.fromLTRB(12, 12, 12, 16),
      children: [
        _buildErrorCard(_newsError),
        Container(
          padding: const EdgeInsets.fromLTRB(14, 12, 14, 12),
          decoration: BoxDecoration(
            color: Colors.white,
            borderRadius: BorderRadius.circular(12),
          ),
          child: Row(
            children: [
              const Icon(Icons.newspaper_rounded, color: Color(0xFF2E5ED7)),
              const SizedBox(width: 8),
              Expanded(
                child: Text(
                  "市场资讯 ${data?.generatedAt.isNotEmpty == true ? "· ${data!.generatedAt}" : ""}",
                  style: const TextStyle(
                    fontSize: 14,
                    fontWeight: FontWeight.w700,
                    color: Color(0xFF1F2A44),
                  ),
                ),
              ),
              Text(
                data?.source.isNotEmpty == true ? data!.source : "-",
                style: const TextStyle(fontSize: 12, color: Color(0xFF7B8599)),
              ),
            ],
          ),
        ),
        const SizedBox(height: 10),
        Container(
          decoration: BoxDecoration(
            color: Colors.white,
            borderRadius: BorderRadius.circular(12),
          ),
          child: items.isEmpty
              ? const Padding(
                  padding: EdgeInsets.all(16),
                  child: Text(
                    "暂无新闻内容，请稍后下拉刷新。",
                    style: TextStyle(color: Color(0xFF7B8599)),
                  ),
                )
              : Column(
                  children: items
                      .take(80)
                      .map(
                        (item) => Container(
                          padding: const EdgeInsets.fromLTRB(12, 10, 12, 10),
                          decoration: const BoxDecoration(
                            border: Border(
                              bottom: BorderSide(color: Color(0xFFF0F2F7)),
                            ),
                          ),
                          child: Column(
                            crossAxisAlignment: CrossAxisAlignment.start,
                            children: [
                              Text(
                                item.title,
                                maxLines: 2,
                                overflow: TextOverflow.ellipsis,
                                style: const TextStyle(
                                  fontSize: 15,
                                  fontWeight: FontWeight.w700,
                                  color: Color(0xFF1F2A44),
                                ),
                              ),
                              if (item.summary.trim().isNotEmpty) ...[
                                const SizedBox(height: 6),
                                Text(
                                  item.summary.trim(),
                                  maxLines: 3,
                                  overflow: TextOverflow.ellipsis,
                                  style: const TextStyle(
                                    fontSize: 13,
                                    color: Color(0xFF5D6578),
                                    height: 1.35,
                                  ),
                                ),
                              ],
                              const SizedBox(height: 6),
                              Row(
                                children: [
                                  Text(
                                    item.mediaName.trim().isEmpty
                                        ? "财经资讯"
                                        : item.mediaName.trim(),
                                    style: const TextStyle(
                                      fontSize: 12,
                                      color: Color(0xFF7B8599),
                                    ),
                                  ),
                                  const SizedBox(width: 8),
                                  Expanded(
                                    child: Text(
                                      item.ctime.trim(),
                                      maxLines: 1,
                                      overflow: TextOverflow.ellipsis,
                                      style: const TextStyle(
                                        fontSize: 12,
                                        color: Color(0xFF9AA1B2),
                                      ),
                                    ),
                                  ),
                                ],
                              ),
                            ],
                          ),
                        ),
                      )
                      .toList(),
                ),
        ),
      ],
    );
  }

  Widget _buildMeTab(BuildContext context) {
    final topInset = MediaQuery.of(context).padding.top;
    return ListView(
      physics: const AlwaysScrollableScrollPhysics(
        parent: ClampingScrollPhysics(),
      ),
      padding: EdgeInsets.fromLTRB(12, topInset + 10, 12, 16),
      children: [
        _buildErrorCard(_portfolioError ?? _watchlistError ?? _sectorError),
        _buildMeProfileCard(),
        if (_accountsError != null) ...[
          const SizedBox(height: 8),
          Text(
            "账户加载失败: $_accountsError",
            style: TextStyle(color: Colors.red.shade700, fontSize: 12),
          ),
        ],
        const SizedBox(height: 12),
        _buildMeSection(
          [
            _buildMeRow(
              icon: Icons.notifications_none_rounded,
              title: "消息提醒设置",
              onTap: () => _showFeatureHint("消息提醒设置"),
            ),
            _buildMeRow(
              icon: Icons.tune_rounded,
              title: "显示设置",
              onTap: () => _showFeatureHint("显示设置"),
            ),
            _buildMeRow(
              icon: Icons.sync_alt_rounded,
              title: "数据源切换",
              value: _quoteSourceLabel(_quoteSourceMode),
              onTap: () => unawaited(_onSwitchQuoteSourcePressed()),
              showDivider: false,
            ),
          ],
        ),
        const SizedBox(height: 12),
        _buildMeSection(
          [
            _buildMeRow(
              icon: Icons.pie_chart_outline_rounded,
              title: "盈亏分析",
              onTap: () {
                setState(() {
                  _tabIndex = 0;
                });
              },
            ),
            _buildMeRow(
              icon: Icons.support_agent_outlined,
              title: "会员客服",
              onTap: () => _showFeatureHint("会员客服"),
            ),
            _buildMeRow(
              icon: Icons.feedback_outlined,
              title: "反馈建议",
              onTap: () => _showFeatureHint("反馈建议"),
            ),
            _buildMeRow(
              icon: Icons.info_outline_rounded,
              title: "关于我们",
              onTap: _showAppInfoDialog,
              showDivider: false,
            ),
          ],
        ),
        const SizedBox(height: 12),
      ],
    );
  }

  @override
  Widget build(BuildContext context) {
    final tabTitles = <String>["持有", "自选", "资金流", "新闻", "我的"];

    Widget body;
    switch (_tabIndex) {
      case 0:
        body = _buildHoldingsTab(context);
      case 1:
        body = _buildRecommendationsTab(context);
      case 2:
        body = _buildFlowTab(context);
      case 3:
        body = _buildNewsTab(context);
      case 4:
        body = _buildMeTab(context);
      default:
        body = _buildHoldingsTab(context);
    }

    return AnnotatedRegion<SystemUiOverlayStyle>(
      value: const SystemUiOverlayStyle(
        statusBarColor: Colors.white,
        statusBarIconBrightness: Brightness.dark,
        statusBarBrightness: Brightness.light,
      ),
      child: Scaffold(
        backgroundColor: const Color(0xFFF3F5F9),
        appBar: _tabIndex == 0 || _tabIndex == 4
            ? null
            : AppBar(
                title: Text(tabTitles[_tabIndex]),
              ),
        body: _tabIndex == 0 || _tabIndex == 4
            ? body
            : RefreshIndicator(
                onRefresh: () => _reload(forceRefresh: true),
                child: body,
              ),
        bottomNavigationBar: DecoratedBox(
          decoration: const BoxDecoration(
            color: Colors.white,
            border: Border(
              top: BorderSide(color: Color(0xFFE6EAF2)),
            ),
            boxShadow: [
              BoxShadow(
                color: Color(0x12000000),
                blurRadius: 10,
                offset: Offset(0, -2),
              ),
            ],
          ),
          child: SafeArea(
            top: false,
            child: Column(
              mainAxisSize: MainAxisSize.min,
              children: [
                if (_tabIndex == 0) ...[
                  _buildMajorIndicesStrip(),
                  const Divider(
                      height: 1, thickness: 1, color: Color(0xFFE9EDF4)),
                ],
                BottomNavigationBar(
                  currentIndex: _tabIndex,
                  type: BottomNavigationBarType.fixed,
                  backgroundColor: Colors.transparent,
                  elevation: 0,
                  selectedItemColor: const Color(0xFF2E5ED7),
                  unselectedItemColor: const Color(0xFF7B8599),
                  onTap: (index) {
                    setState(() {
                      _tabIndex = index;
                    });
                    unawaited(_refreshCurrentTabSilently());
                  },
                  items: const [
                    BottomNavigationBarItem(
                      icon: Icon(Icons.account_balance_wallet_outlined),
                      activeIcon: Icon(Icons.account_balance_wallet),
                      label: "持有",
                    ),
                    BottomNavigationBarItem(
                      icon: Icon(Icons.lightbulb_outline),
                      activeIcon: Icon(Icons.lightbulb),
                      label: "自选",
                    ),
                    BottomNavigationBarItem(
                      icon: Icon(Icons.show_chart_outlined),
                      activeIcon: Icon(Icons.show_chart),
                      label: "资金流",
                    ),
                    BottomNavigationBarItem(
                      icon: Icon(Icons.article_outlined),
                      activeIcon: Icon(Icons.article),
                      label: "新闻",
                    ),
                    BottomNavigationBarItem(
                      icon: Icon(Icons.person_outline),
                      activeIcon: Icon(Icons.person),
                      label: "我的",
                    ),
                  ],
                ),
              ],
            ),
          ),
        ),
      ),
    );
  }
}

class _FundSearchPage extends StatefulWidget {
  const _FundSearchPage({
    required this.apiClient,
    required this.initialKeyword,
    required this.initialHistory,
    required this.onClearHistory,
  });

  final ApiClient apiClient;
  final String initialKeyword;
  final List<FundSearchResult> initialHistory;
  final Future<void> Function() onClearHistory;

  @override
  State<_FundSearchPage> createState() => _FundSearchPageState();
}

class _FundSearchPageState extends State<_FundSearchPage> {
  late final TextEditingController _controller;
  Timer? _debounce;
  bool _loading = false;
  String? _error;
  List<FundSearchResult> _results = const [];
  List<FundSearchResult> _history = const [];

  @override
  void initState() {
    super.initState();
    _controller = TextEditingController(text: widget.initialKeyword);
    _history = widget.initialHistory;
    if (widget.initialKeyword.trim().isNotEmpty) {
      unawaited(_runSearch(widget.initialKeyword));
    }
  }

  @override
  void dispose() {
    _debounce?.cancel();
    _controller.dispose();
    super.dispose();
  }

  void _onChanged(String text) {
    _debounce?.cancel();
    _debounce = Timer(const Duration(milliseconds: 260), () {
      unawaited(_runSearch(text));
    });
  }

  Future<void> _runSearch(String raw) async {
    final keyword = raw.trim();
    if (keyword.isEmpty) {
      if (!mounted) {
        return;
      }
      setState(() {
        _results = const [];
        _error = null;
        _loading = false;
      });
      return;
    }

    setState(() {
      _loading = true;
      _error = null;
    });
    try {
      final items = await widget.apiClient.searchFunds(keyword, limit: 30);
      if (!mounted) {
        return;
      }
      setState(() {
        _results = items;
        _loading = false;
      });
    } catch (error) {
      if (!mounted) {
        return;
      }
      setState(() {
        _error = error.toString();
        _loading = false;
      });
    }
  }

  Future<void> _clearHistory() async {
    await widget.onClearHistory();
    if (!mounted) {
      return;
    }
    setState(() {
      _history = const [];
    });
  }

  void _pick(FundSearchResult item) {
    Navigator.of(context).pop(item);
  }

  @override
  Widget build(BuildContext context) {
    final keyword = _controller.text.trim();
    final showHistory = keyword.isEmpty;
    return Scaffold(
      appBar: AppBar(
        title: const Text("搜索基金"),
      ),
      body: Column(
        children: [
          Padding(
            padding: const EdgeInsets.fromLTRB(12, 10, 12, 10),
            child: TextField(
              controller: _controller,
              autofocus: true,
              onChanged: _onChanged,
              decoration: InputDecoration(
                hintText: "输入基金代码或名称",
                prefixIcon: const Icon(Icons.search_rounded),
                suffixIcon: keyword.isEmpty
                    ? null
                    : IconButton(
                        onPressed: () {
                          _controller.clear();
                          _onChanged("");
                        },
                        icon: const Icon(Icons.close_rounded),
                      ),
                border: const OutlineInputBorder(),
                isDense: true,
              ),
            ),
          ),
          Expanded(
            child: Builder(
              builder: (_) {
                if (_loading) {
                  return const Center(child: CircularProgressIndicator());
                }
                if (_error != null && _error!.isNotEmpty) {
                  return Center(
                    child: Padding(
                      padding: const EdgeInsets.symmetric(horizontal: 20),
                      child: Text(
                        "搜索失败：$_error",
                        style: const TextStyle(color: Color(0xFFB42318)),
                      ),
                    ),
                  );
                }

                if (showHistory) {
                  if (_history.isEmpty) {
                    return const Center(
                      child: Text(
                        "暂无搜索历史",
                        style: TextStyle(color: Color(0xFF7B8599)),
                      ),
                    );
                  }
                  return Column(
                    children: [
                      Padding(
                        padding: const EdgeInsets.fromLTRB(12, 4, 6, 4),
                        child: Row(
                          children: [
                            const Text(
                              "搜索历史",
                              style: TextStyle(
                                fontWeight: FontWeight.w700,
                                color: Color(0xFF1F2A44),
                              ),
                            ),
                            const Spacer(),
                            TextButton(
                              onPressed: _clearHistory,
                              child: const Text("清空"),
                            ),
                          ],
                        ),
                      ),
                      Expanded(
                        child: ListView.builder(
                          itemCount: _history.length,
                          itemBuilder: (context, index) {
                            final item = _history[index];
                            return ListTile(
                              leading: const Icon(Icons.history,
                                  color: Color(0xFF9AA1B2)),
                              title: Text(
                                  item.name.isEmpty ? item.code : item.name),
                              subtitle: Text(item.code),
                              onTap: () => _pick(item),
                            );
                          },
                        ),
                      ),
                    ],
                  );
                }

                if (_results.isEmpty) {
                  return const Center(
                    child: Text(
                      "未找到匹配基金",
                      style: TextStyle(color: Color(0xFF7B8599)),
                    ),
                  );
                }

                return ListView.builder(
                  itemCount: _results.length,
                  itemBuilder: (context, index) {
                    final item = _results[index];
                    return ListTile(
                      leading: const Icon(Icons.search_rounded),
                      title: Text(item.name.isEmpty ? item.code : item.name),
                      subtitle: Text(item.code),
                      onTap: () => _pick(item),
                    );
                  },
                );
              },
            ),
          ),
        ],
      ),
    );
  }
}

class _FundTrendPage extends StatefulWidget {
  const _FundTrendPage({
    required this.apiClient,
    required this.code,
    required this.name,
    required this.quoteSource,
    required this.position,
    required this.accountTotalAsset,
  });

  final ApiClient apiClient;
  final String code;
  final String name;
  final String quoteSource;
  final PortfolioPosition position;
  final double? accountTotalAsset;

  @override
  State<_FundTrendPage> createState() => _FundTrendPageState();
}

class _FundTrendPageState extends State<_FundTrendPage> {
  FundTrendResponse? _trend;
  String? _error;
  bool _loading = false;
  int _sectionTab = 0;
  final NumberFormat _moneyFmt = NumberFormat("#,##0.00");
  static const List<String> _sectionTabs = <String>["关联板块", "业绩走势", "我的收益"];

  @override
  void initState() {
    super.initState();
    _load();
  }

  Future<void> _load() async {
    setState(() {
      _loading = true;
      _error = null;
    });
    try {
      final trend = await widget.apiClient.fetchFundTrend(
        code: widget.code,
        quoteSource: widget.quoteSource,
        maxPoints: 220,
      );
      if (!mounted) {
        return;
      }
      if (!trend.ok || trend.points.isEmpty) {
        setState(() {
          _trend = trend;
          _error = trend.error.isEmpty ? "暂无走势数据" : trend.error;
        });
        return;
      }
      setState(() {
        _trend = trend;
        _error = null;
      });
    } catch (error) {
      if (!mounted) {
        return;
      }
      String message = error.toString();
      if (error is ApiException && error.body.isNotEmpty) {
        message = "${error.message}\n${error.body}";
      }
      setState(() {
        _error = message;
      });
    } finally {
      if (mounted) {
        setState(() {
          _loading = false;
        });
      }
    }
  }

  String _fmtNum(double value, {int digits = 4}) =>
      value.toStringAsFixed(digits);

  String _fmtMoney(double? value) {
    if (value == null) {
      return "--";
    }
    return _moneyFmt.format(value);
  }

  String _fmtPct(double? value) {
    if (value == null) {
      return "--";
    }
    return "${value >= 0 ? "+" : ""}${value.toStringAsFixed(2)}%";
  }

  TextStyle _signedStyle(double? value, {double size = 13}) {
    final color =
        (value ?? 0) >= 0 ? const Color(0xFFDE4C54) : const Color(0xFF17A34A);
    return TextStyle(
      color: color,
      fontSize: size,
      fontWeight: FontWeight.w700,
    );
  }

  Widget _gridMetric(
    String label,
    String value, {
    TextStyle? valueStyle,
  }) {
    return Column(
      crossAxisAlignment: CrossAxisAlignment.start,
      children: [
        Text(
          label,
          style: const TextStyle(
            fontSize: 13,
            color: Color(0xFF7B8599),
            fontWeight: FontWeight.w500,
          ),
        ),
        const SizedBox(height: 5),
        Text(
          value,
          style: valueStyle ??
              const TextStyle(
                fontSize: 16,
                color: Color(0xFF1F2A44),
                fontWeight: FontWeight.w700,
              ),
        ),
      ],
    );
  }

  Widget _summaryMetric(String label, String value, {TextStyle? valueStyle}) {
    return Column(
      crossAxisAlignment: CrossAxisAlignment.start,
      children: [
        Text(
          label,
          style: const TextStyle(
            color: Color(0xFF66728C),
            fontSize: 12,
            fontWeight: FontWeight.w600,
          ),
        ),
        const SizedBox(height: 4),
        Text(
          value,
          style: valueStyle ??
              const TextStyle(
                color: Color(0xFF1F2A44),
                fontSize: 16,
                fontWeight: FontWeight.w800,
              ),
        ),
      ],
    );
  }

  Widget _buildTopHero({
    required String title,
    required String code,
  }) {
    return Container(
      padding: const EdgeInsets.fromLTRB(14, 14, 14, 12),
      decoration: BoxDecoration(
        borderRadius: BorderRadius.circular(16),
        gradient: const LinearGradient(
          begin: Alignment.topLeft,
          end: Alignment.bottomRight,
          colors: [Color(0xFF2254E6), Color(0xFF2E67FF)],
        ),
      ),
      child: Column(
        crossAxisAlignment: CrossAxisAlignment.start,
        children: [
          Text(
            title,
            maxLines: 1,
            overflow: TextOverflow.ellipsis,
            style: const TextStyle(
              color: Colors.white,
              fontSize: 21,
              fontWeight: FontWeight.w800,
            ),
          ),
          const SizedBox(height: 4),
          Text(
            code,
            style: const TextStyle(
              color: Color(0xFFD9E6FF),
              fontSize: 16,
              fontWeight: FontWeight.w600,
            ),
          ),
          const SizedBox(height: 12),
          Container(
            width: double.infinity,
            padding: const EdgeInsets.symmetric(horizontal: 10, vertical: 8),
            decoration: BoxDecoration(
              borderRadius: BorderRadius.circular(10),
              color: const Color(0x24FFFFFF),
              border: Border.all(color: const Color(0x3DFFFFFF)),
            ),
            child: const Row(
              children: [
                Icon(Icons.auto_graph_rounded, size: 14, color: Colors.white),
                SizedBox(width: 6),
                Expanded(
                  child: Text(
                    "自选风向标 | 今日加自选榜",
                    style: TextStyle(
                      color: Colors.white,
                      fontSize: 12,
                      fontWeight: FontWeight.w600,
                    ),
                  ),
                ),
              ],
            ),
          ),
        ],
      ),
    );
  }

  Widget _buildSectionTabs() {
    return Container(
      padding: const EdgeInsets.all(4),
      decoration: BoxDecoration(
        color: const Color(0xFFF1F4FA),
        borderRadius: BorderRadius.circular(10),
      ),
      child: Row(
        children: List.generate(_sectionTabs.length, (index) {
          final active = _sectionTab == index;
          return Expanded(
            child: InkWell(
              borderRadius: BorderRadius.circular(8),
              onTap: () {
                setState(() {
                  _sectionTab = index;
                });
              },
              child: AnimatedContainer(
                duration: const Duration(milliseconds: 180),
                padding: const EdgeInsets.symmetric(vertical: 8),
                decoration: BoxDecoration(
                  color: active ? Colors.white : Colors.transparent,
                  borderRadius: BorderRadius.circular(8),
                  boxShadow: active
                      ? const [
                          BoxShadow(
                            color: Color(0x12000000),
                            blurRadius: 6,
                            offset: Offset(0, 2),
                          )
                        ]
                      : const [],
                ),
                child: Text(
                  _sectionTabs[index],
                  textAlign: TextAlign.center,
                  style: TextStyle(
                    fontSize: 14,
                    fontWeight: FontWeight.w700,
                    color: active
                        ? const Color(0xFF1F2A44)
                        : const Color(0xFF7B8599),
                  ),
                ),
              ),
            ),
          );
        }),
      ),
    );
  }

  Widget _buildTrendChartCard(List<FundTrendPoint> points) {
    return Container(
      padding: const EdgeInsets.fromLTRB(10, 10, 10, 12),
      decoration: BoxDecoration(
        color: Colors.white,
        borderRadius: BorderRadius.circular(14),
        border: Border.all(color: const Color(0xFFE7EBF3)),
      ),
      child: Column(
        children: [
          SizedBox(
            height: 220,
            width: double.infinity,
            child: CustomPaint(
              painter: _FundTrendPainter(points: points),
            ),
          ),
          const SizedBox(height: 8),
          Row(
            children: [
              Text(
                points.first.time,
                style: const TextStyle(
                  color: Color(0xFF7B8599),
                  fontSize: 12,
                ),
              ),
              const Spacer(),
              Text(
                points[(points.length / 2).floor()].time,
                style: const TextStyle(
                  color: Color(0xFF7B8599),
                  fontSize: 12,
                ),
              ),
              const Spacer(),
              Text(
                points.last.time,
                style: const TextStyle(
                  color: Color(0xFF7B8599),
                  fontSize: 12,
                ),
              ),
            ],
          ),
        ],
      ),
    );
  }

  Widget _buildActionBar() {
    Widget button(IconData icon, String text) {
      return Expanded(
        child: InkWell(
          onTap: () {
            ScaffoldMessenger.of(context).showSnackBar(
              SnackBar(content: Text("$text 功能开发中")),
            );
          },
          child: Padding(
            padding: const EdgeInsets.symmetric(vertical: 8),
            child: Column(
              mainAxisSize: MainAxisSize.min,
              children: [
                Icon(icon, size: 21, color: const Color(0xFF2A344E)),
                const SizedBox(height: 4),
                Text(
                  text,
                  style: const TextStyle(
                    fontSize: 12,
                    color: Color(0xFF2A344E),
                    fontWeight: FontWeight.w500,
                  ),
                ),
              ],
            ),
          ),
        ),
      );
    }

    return Container(
      decoration: const BoxDecoration(
        color: Colors.white,
        border: Border(top: BorderSide(color: Color(0xFFE7EBF3))),
      ),
      child: SafeArea(
        top: false,
        child: Row(
          children: [
            button(Icons.edit_note_rounded, "修改持仓"),
            button(Icons.notifications_active_outlined, "提醒"),
            button(Icons.event_note_rounded, "交易记录"),
            button(Icons.remove_circle_outline_rounded, "删自选"),
            button(Icons.more_horiz_rounded, "更多"),
          ],
        ),
      ),
    );
  }

  @override
  Widget build(BuildContext context) {
    final title = widget.name.trim().isEmpty ? widget.code : widget.name.trim();
    final p = widget.position;
    final trend = _trend;
    final points = trend?.points ?? const <FundTrendPoint>[];
    final hasData = points.isNotEmpty;

    double latestNav = 0;
    double latestPct = 0;
    String latestTime = "--";
    double minNav = 0;
    double maxNav = 0;
    final holdAmount = p.marketValue;
    final holdRatio =
        (holdAmount != null && (widget.accountTotalAsset ?? 0) > 0)
            ? holdAmount / (widget.accountTotalAsset ?? 1) * 100
            : null;
    if (hasData) {
      latestNav = points.last.nav;
      latestPct = points.last.pct ?? 0;
      latestTime = points.last.time;
      minNav = points.map((e) => e.nav).reduce(math.min);
      maxNav = points.map((e) => e.nav).reduce(math.max);
    }

    return Scaffold(
      backgroundColor: const Color(0xFFF3F5F9),
      appBar: AppBar(
        backgroundColor: const Color(0xFF275BEB),
        foregroundColor: Colors.white,
        elevation: 0,
        title: Column(
          crossAxisAlignment: CrossAxisAlignment.start,
          children: [
            Text(
              title,
              maxLines: 1,
              overflow: TextOverflow.ellipsis,
              style: const TextStyle(fontSize: 16, fontWeight: FontWeight.w700),
            ),
            Text(
              widget.code,
              style: const TextStyle(
                fontSize: 13,
                fontWeight: FontWeight.w500,
                color: Color(0xFFD9E6FF),
              ),
            ),
          ],
        ),
        actions: [
          IconButton(
            onPressed: _loading ? null : _load,
            icon: const Icon(Icons.refresh_rounded),
          ),
        ],
      ),
      bottomNavigationBar: _buildActionBar(),
      body: _loading && !hasData
          ? const Center(child: CircularProgressIndicator())
          : RefreshIndicator(
              onRefresh: _load,
              child: ListView(
                physics: const AlwaysScrollableScrollPhysics(
                  parent: ClampingScrollPhysics(),
                ),
                padding: const EdgeInsets.fromLTRB(12, 12, 12, 22),
                children: [
                  _buildTopHero(title: title, code: widget.code),
                  const SizedBox(height: 10),
                  if (_error != null && _error!.isNotEmpty)
                    Container(
                      margin: const EdgeInsets.only(bottom: 10),
                      padding: const EdgeInsets.all(12),
                      decoration: BoxDecoration(
                        color: const Color(0xFFFFF1F2),
                        borderRadius: BorderRadius.circular(12),
                        border: Border.all(color: const Color(0xFFFECACA)),
                      ),
                      child: Text(
                        _error!,
                        style: const TextStyle(
                          color: Color(0xFFB42318),
                          fontSize: 13,
                        ),
                      ),
                    ),
                  if (!hasData)
                    Container(
                      padding: const EdgeInsets.all(20),
                      decoration: BoxDecoration(
                        color: Colors.white,
                        borderRadius: BorderRadius.circular(14),
                      ),
                      child: const Text(
                        "暂无走势数据，下拉重试。",
                        style: TextStyle(color: Color(0xFF7B8599)),
                      ),
                    )
                  else ...[
                    Container(
                      padding: const EdgeInsets.fromLTRB(14, 12, 14, 12),
                      decoration: BoxDecoration(
                        color: Colors.white,
                        borderRadius: BorderRadius.circular(14),
                        border: Border.all(color: const Color(0xFFE7EBF3)),
                      ),
                      child: Row(
                        children: [
                          Expanded(
                            child: _summaryMetric(
                              "当日涨幅",
                              _fmtPct(latestPct),
                              valueStyle: _signedStyle(latestPct, size: 15),
                            ),
                          ),
                          Expanded(
                            child: _summaryMetric(
                              "最新估值",
                              _fmtNum(latestNav),
                              valueStyle: const TextStyle(
                                color: Color(0xFF1F2A44),
                                fontSize: 15,
                                fontWeight: FontWeight.w800,
                              ),
                            ),
                          ),
                          Expanded(
                            child: _summaryMetric(
                              "更新时间",
                              latestTime,
                              valueStyle: const TextStyle(
                                color: Color(0xFF1F2A44),
                                fontSize: 15,
                                fontWeight: FontWeight.w800,
                              ),
                            ),
                          ),
                        ],
                      ),
                    ),
                    const SizedBox(height: 10),
                    Container(
                      padding: const EdgeInsets.fromLTRB(14, 12, 14, 12),
                      decoration: BoxDecoration(
                        color: Colors.white,
                        borderRadius: BorderRadius.circular(14),
                        border: Border.all(color: const Color(0xFFE7EBF3)),
                      ),
                      child: GridView.count(
                        crossAxisCount: 3,
                        shrinkWrap: true,
                        physics: const NeverScrollableScrollPhysics(),
                        childAspectRatio: 2.25,
                        mainAxisSpacing: 8,
                        crossAxisSpacing: 10,
                        children: [
                          _gridMetric("持有金额", _fmtMoney(holdAmount)),
                          _gridMetric("持有份额", _fmtNum(p.shares, digits: 2)),
                          _gridMetric("持仓占比", _fmtPct(holdRatio)),
                          _gridMetric(
                            "持有收益",
                            p.holdingProfit == null
                                ? "--"
                                : "${(p.holdingProfit ?? 0) >= 0 ? "+" : ""}${_fmtMoney(p.holdingProfit)}",
                            valueStyle: _signedStyle(p.holdingProfit),
                          ),
                          _gridMetric(
                            "持有收益率",
                            _fmtPct(p.holdingProfitPct),
                            valueStyle: _signedStyle(p.holdingProfitPct),
                          ),
                          _gridMetric("持仓成本", _fmtNum(p.cost)),
                          _gridMetric(
                            "当日收益",
                            p.dailyProfit == null
                                ? "--"
                                : "${(p.dailyProfit ?? 0) >= 0 ? "+" : ""}${_fmtMoney(p.dailyProfit)}",
                            valueStyle: _signedStyle(p.dailyProfit),
                          ),
                          _gridMetric("昨日收益", "--"),
                          _gridMetric("持有天数", "--"),
                        ],
                      ),
                    ),
                    const SizedBox(height: 10),
                    _buildSectionTabs(),
                    const SizedBox(height: 10),
                    if (_sectionTab == 0) ...[
                      _buildTrendChartCard(points),
                      const SizedBox(height: 10),
                      Container(
                        padding: const EdgeInsets.symmetric(
                          horizontal: 12,
                          vertical: 11,
                        ),
                        decoration: BoxDecoration(
                          color: Colors.white,
                          borderRadius: BorderRadius.circular(12),
                          border: Border.all(color: const Color(0xFFE7EBF3)),
                        ),
                        child: Row(
                          children: [
                            const Text(
                              "关联板块",
                              style: TextStyle(
                                color: Color(0xFF1F2A44),
                                fontSize: 16,
                                fontWeight: FontWeight.w700,
                              ),
                            ),
                            const Spacer(),
                            Text(
                              p.sector.isEmpty ? "未知板块" : p.sector,
                              style: const TextStyle(
                                color: Color(0xFF1F2A44),
                                fontSize: 15,
                                fontWeight: FontWeight.w600,
                              ),
                            ),
                            const SizedBox(width: 8),
                            Text(
                              _fmtPct(p.sectorPct),
                              style: _signedStyle(p.sectorPct),
                            ),
                          ],
                        ),
                      ),
                    ] else if (_sectionTab == 1) ...[
                      _buildTrendChartCard(points),
                      const SizedBox(height: 10),
                      Container(
                        padding: const EdgeInsets.symmetric(
                          horizontal: 12,
                          vertical: 10,
                        ),
                        decoration: BoxDecoration(
                          color: Colors.white,
                          borderRadius: BorderRadius.circular(12),
                          border: Border.all(color: const Color(0xFFE7EBF3)),
                        ),
                        child: Row(
                          children: [
                            _summaryMetric("日内最低", _fmtNum(minNav)),
                            const Spacer(),
                            _summaryMetric("日内最高", _fmtNum(maxNav)),
                          ],
                        ),
                      ),
                    ] else ...[
                      _buildTrendChartCard(points),
                      const SizedBox(height: 10),
                      Container(
                        padding: const EdgeInsets.all(12),
                        decoration: BoxDecoration(
                          color: Colors.white,
                          borderRadius: BorderRadius.circular(12),
                          border: Border.all(color: const Color(0xFFE7EBF3)),
                        ),
                        child: Column(
                          children: [
                            Row(
                              children: [
                                const Text(
                                  "累计收益",
                                  style: TextStyle(
                                    color: Color(0xFF7B8599),
                                    fontSize: 13,
                                  ),
                                ),
                                const Spacer(),
                                Text(
                                  p.holdingProfit == null
                                      ? "--"
                                      : "${(p.holdingProfit ?? 0) >= 0 ? "+" : ""}${_fmtMoney(p.holdingProfit)}",
                                  style:
                                      _signedStyle(p.holdingProfit, size: 16),
                                ),
                              ],
                            ),
                            const SizedBox(height: 8),
                            Row(
                              children: [
                                const Text(
                                  "累计收益率",
                                  style: TextStyle(
                                    color: Color(0xFF7B8599),
                                    fontSize: 13,
                                  ),
                                ),
                                const Spacer(),
                                Text(
                                  _fmtPct(p.holdingProfitPct),
                                  style: _signedStyle(p.holdingProfitPct,
                                      size: 16),
                                ),
                              ],
                            ),
                          ],
                        ),
                      ),
                    ],
                    const SizedBox(height: 10),
                    Container(
                      padding: const EdgeInsets.fromLTRB(12, 10, 12, 10),
                      decoration: BoxDecoration(
                        color: Colors.white,
                        borderRadius: BorderRadius.circular(14),
                        border: Border.all(color: const Color(0xFFE7EBF3)),
                      ),
                      child: Text(
                        "数据源: ${trend?.source.isNotEmpty == true ? trend!.source : "unknown"}"
                        "${trend?.generatedAt.isNotEmpty == true ? " | 生成于 ${trend!.generatedAt}" : ""}",
                        style: const TextStyle(
                          color: Color(0xFF7B8599),
                          fontSize: 12,
                        ),
                      ),
                    ),
                    const SizedBox(height: 28),
                  ],
                ],
              ),
            ),
    );
  }
}

class _FundTrendPainter extends CustomPainter {
  const _FundTrendPainter({required this.points});

  final List<FundTrendPoint> points;

  @override
  void paint(Canvas canvas, Size size) {
    final bg = Paint()..color = const Color(0xFFF8FAFF);
    canvas.drawRRect(
      RRect.fromRectAndRadius(Offset.zero & size, const Radius.circular(10)),
      bg,
    );

    if (points.isEmpty) {
      return;
    }
    if (points.length == 1) {
      final dot = Paint()..color = const Color(0xFF2457FF);
      canvas.drawCircle(
        Offset(size.width * 0.5, size.height * 0.5),
        3.5,
        dot,
      );
      return;
    }

    final navs = points.map((e) => e.nav).toList();
    final minNav = navs.reduce(math.min);
    final maxNav = navs.reduce(math.max);
    final range = (maxNav - minNav).abs() < 1e-9 ? 1e-9 : (maxNav - minNav);

    final gridPaint = Paint()
      ..color = const Color(0xFFE6EBF6)
      ..strokeWidth = 1;
    for (var i = 1; i <= 4; i++) {
      final y = size.height * i / 5;
      canvas.drawLine(Offset(0, y), Offset(size.width, y), gridPaint);
    }

    final path = Path();
    final fillPath = Path();
    for (var i = 0; i < points.length; i++) {
      final x = size.width * i / (points.length - 1);
      final y = size.height - ((points[i].nav - minNav) / range) * size.height;
      if (i == 0) {
        path.moveTo(x, y);
        fillPath.moveTo(x, size.height);
        fillPath.lineTo(x, y);
      } else {
        path.lineTo(x, y);
        fillPath.lineTo(x, y);
      }
    }
    fillPath.lineTo(size.width, size.height);
    fillPath.close();

    final fillPaint = Paint()
      ..shader = const LinearGradient(
        begin: Alignment.topCenter,
        end: Alignment.bottomCenter,
        colors: [Color(0x33407CFF), Color(0x00407CFF)],
      ).createShader(Offset.zero & size);
    canvas.drawPath(fillPath, fillPaint);

    final linePaint = Paint()
      ..color = const Color(0xFF2E5ED7)
      ..strokeWidth = 2
      ..style = PaintingStyle.stroke
      ..strokeCap = StrokeCap.round
      ..strokeJoin = StrokeJoin.round;
    canvas.drawPath(path, linePaint);

    final latestX = size.width;
    final latestY =
        size.height - ((points.last.nav - minNav) / range) * size.height;
    canvas.drawCircle(
      Offset(latestX, latestY),
      3.5,
      Paint()..color = const Color(0xFF2E5ED7),
    );
    canvas.drawCircle(
      Offset(latestX, latestY),
      6.5,
      Paint()..color = const Color(0x332E5ED7),
    );
  }

  @override
  bool shouldRepaint(covariant _FundTrendPainter oldDelegate) {
    if (oldDelegate.points.length != points.length) {
      return true;
    }
    if (points.isEmpty) {
      return false;
    }
    return oldDelegate.points.last.nav != points.last.nav;
  }
}

class _FundAnalysisPage extends StatefulWidget {
  const _FundAnalysisPage({
    required this.apiClient,
    required this.code,
    required this.name,
    required this.quoteSource,
  });

  final ApiClient apiClient;
  final String code;
  final String name;
  final String quoteSource;

  @override
  State<_FundAnalysisPage> createState() => _FundAnalysisPageState();
}

class _FundAnalysisPageState extends State<_FundAnalysisPage> {
  FundAnalysis? _analysis;
  String? _error;
  bool _loading = false;
  bool _aiLoading = false;
  int _loadSeq = 0;

  @override
  void initState() {
    super.initState();
    _load();
  }

  Future<void> _load() async {
    final int seq = ++_loadSeq;
    setState(() {
      _loading = true;
      _aiLoading = false;
      _error = null;
    });
    try {
      FundAnalysis quickResult;
      bool shouldLoadAi = true;
      try {
        quickResult = await widget.apiClient.analyzeWatchFund(
          code: widget.code,
          name: widget.name,
          quoteSource: widget.quoteSource,
          includeAi: false,
          timeout: const Duration(seconds: 8),
        );
      } catch (e) {
        // Backward compatibility: some servers may ignore include_ai or respond
        // with validation errors. Fall back to a full analyze call so page still works.
        quickResult = await widget.apiClient.analyzeWatchFund(
          code: widget.code,
          name: widget.name,
          quoteSource: widget.quoteSource,
          includeAi: true,
          timeout: const Duration(seconds: 10),
        );
        shouldLoadAi = false;
      }
      if (!mounted || seq != _loadSeq) {
        return;
      }
      setState(() {
        _analysis = quickResult;
        _loading = false;
        _aiLoading = shouldLoadAi;
      });

      if (!shouldLoadAi) {
        return;
      }

      try {
        final fullResult = await widget.apiClient.analyzeWatchFund(
          code: widget.code,
          name: widget.name,
          quoteSource: widget.quoteSource,
          includeAi: true,
          timeout: const Duration(seconds: 10),
        );
        if (!mounted || seq != _loadSeq) {
          return;
        }
        setState(() {
          _analysis = fullResult;
          _aiLoading = false;
        });
      } catch (_) {
        if (!mounted || seq != _loadSeq) {
          return;
        }
        setState(() {
          _aiLoading = false;
        });
      }
    } catch (error) {
      if (!mounted || seq != _loadSeq) {
        return;
      }
      String message = error.toString();
      if (error is ApiException && error.body.isNotEmpty) {
        message = "${error.message}\n${error.body}";
      }
      setState(() {
        _error = message;
        _aiLoading = false;
      });
    } finally {
      if (mounted && seq == _loadSeq) {
        setState(() {
          _loading = false;
        });
      }
    }
  }

  String _fmtNum(double? value, {int digits = 2}) {
    if (value == null) {
      return "--";
    }
    return value.toStringAsFixed(digits);
  }

  Color _actionColor(String action) {
    switch (action.toUpperCase()) {
      case "BUY":
        return const Color(0xFF2457FF);
      case "SELL":
        return const Color(0xFFB42318);
      default:
        return const Color(0xFF6C7387);
    }
  }

  Widget _pill(
    String text, {
    required Color color,
    Color? background,
  }) {
    return Container(
      padding: const EdgeInsets.symmetric(horizontal: 10, vertical: 5),
      decoration: BoxDecoration(
        color: background ?? color.withValues(alpha: 0.12),
        borderRadius: BorderRadius.circular(999),
      ),
      child: Text(
        text,
        style: TextStyle(
          color: color,
          fontSize: 12,
          fontWeight: FontWeight.w700,
        ),
      ),
    );
  }

  Widget _sectionCard({
    required String title,
    required Widget child,
    IconData icon = Icons.article_outlined,
  }) {
    return Container(
      margin: const EdgeInsets.only(top: 12),
      padding: const EdgeInsets.all(14),
      decoration: BoxDecoration(
        color: Colors.white,
        borderRadius: BorderRadius.circular(14),
        border: Border.all(color: const Color(0xFFE7EBF3)),
      ),
      child: Column(
        crossAxisAlignment: CrossAxisAlignment.start,
        children: [
          Row(
            children: [
              Icon(icon, size: 18, color: const Color(0xFF2457FF)),
              const SizedBox(width: 6),
              Text(
                title,
                style: const TextStyle(
                  fontSize: 15,
                  fontWeight: FontWeight.w800,
                  color: Color(0xFF1F2A44),
                ),
              ),
            ],
          ),
          const SizedBox(height: 10),
          child,
        ],
      ),
    );
  }

  @override
  Widget build(BuildContext context) {
    final analysis = _analysis;
    final title = widget.name.trim().isEmpty ? widget.code : widget.name.trim();

    return Scaffold(
      backgroundColor: const Color(0xFFF3F5F9),
      appBar: AppBar(
        title: Text("$title 分析"),
        actions: [
          IconButton(
            onPressed: _loading ? null : _load,
            icon: const Icon(Icons.refresh_rounded),
          ),
        ],
      ),
      body: _loading && analysis == null
          ? const Center(child: CircularProgressIndicator())
          : RefreshIndicator(
              onRefresh: _load,
              child: ListView(
                physics: const AlwaysScrollableScrollPhysics(
                  parent: ClampingScrollPhysics(),
                ),
                padding: const EdgeInsets.fromLTRB(12, 12, 12, 24),
                children: [
                  if (_error != null)
                    Container(
                      margin: const EdgeInsets.only(bottom: 10),
                      padding: const EdgeInsets.all(12),
                      decoration: BoxDecoration(
                        color: const Color(0xFFFFF1F2),
                        borderRadius: BorderRadius.circular(12),
                        border: Border.all(color: const Color(0xFFFECACA)),
                      ),
                      child: Row(
                        crossAxisAlignment: CrossAxisAlignment.start,
                        children: [
                          const Icon(
                            Icons.error_outline_rounded,
                            color: Color(0xFFB42318),
                            size: 18,
                          ),
                          const SizedBox(width: 8),
                          Expanded(
                            child: Text(
                              _error!,
                              style: const TextStyle(
                                color: Color(0xFFB42318),
                                fontSize: 13,
                              ),
                            ),
                          ),
                        ],
                      ),
                    ),
                  if (analysis != null) ...[
                    Container(
                      padding: const EdgeInsets.all(14),
                      decoration: BoxDecoration(
                        borderRadius: BorderRadius.circular(16),
                        gradient: const LinearGradient(
                          begin: Alignment.topLeft,
                          end: Alignment.bottomRight,
                          colors: [Color(0xFF1F44C8), Color(0xFF3C74FF)],
                        ),
                        boxShadow: const [
                          BoxShadow(
                            color: Color(0x332457FF),
                            blurRadius: 16,
                            offset: Offset(0, 8),
                          ),
                        ],
                      ),
                      child: Column(
                        crossAxisAlignment: CrossAxisAlignment.start,
                        children: [
                          Text(
                            "${analysis.name} (${analysis.code})",
                            style: const TextStyle(
                              color: Colors.white,
                              fontSize: 16,
                              fontWeight: FontWeight.w800,
                            ),
                          ),
                          const SizedBox(height: 4),
                          Text(
                            "更新时间: ${analysis.generatedAt}",
                            style: const TextStyle(
                              color: Color(0xFFDCE7FF),
                              fontSize: 12,
                            ),
                          ),
                          const SizedBox(height: 12),
                          Row(
                            crossAxisAlignment: CrossAxisAlignment.end,
                            children: [
                              Text(
                                _fmtNum(analysis.latestPrice, digits: 4),
                                style: const TextStyle(
                                  color: Colors.white,
                                  fontSize: 30,
                                  fontWeight: FontWeight.w800,
                                  height: 1,
                                ),
                              ),
                              const SizedBox(width: 10),
                              _pill(
                                analysis.latestPct == null
                                    ? "--"
                                    : "${analysis.latestPct! >= 0 ? "+" : ""}${analysis.latestPct!.toStringAsFixed(2)}%",
                                color: Colors.white,
                                background: const Color(0x33FFFFFF),
                              ),
                              const Spacer(),
                              _pill(
                                "AI ${analysis.aiAction}",
                                color: Colors.white,
                                background: const Color(0x26FFFFFF),
                              ),
                            ],
                          ),
                          if (_aiLoading) ...[
                            const SizedBox(height: 12),
                            const LinearProgressIndicator(
                              minHeight: 2,
                              color: Colors.white,
                              backgroundColor: Color(0x55FFFFFF),
                            ),
                            const SizedBox(height: 6),
                            const Text(
                              "AI 分析更新中...",
                              style: TextStyle(
                                fontSize: 12,
                                color: Color(0xFFE7EEFF),
                              ),
                            ),
                          ],
                        ],
                      ),
                    ),
                    _sectionCard(
                      title: "策略信号",
                      icon: Icons.track_changes_outlined,
                      child: Column(
                        crossAxisAlignment: CrossAxisAlignment.start,
                        children: [
                          Wrap(
                            spacing: 8,
                            runSpacing: 8,
                            children: [
                              _pill(
                                "动作 ${analysis.signalAction}",
                                color: _actionColor(analysis.signalAction),
                              ),
                              _pill(
                                "仓位 ${analysis.signalPositionHint}",
                                color: const Color(0xFF2457FF),
                              ),
                              if (analysis.basePrice != null)
                                _pill(
                                  "中枢 ${analysis.basePrice!.toStringAsFixed(4)}",
                                  color: const Color(0xFF6C7387),
                                ),
                            ],
                          ),
                          if (analysis.grids.isNotEmpty) ...[
                            const SizedBox(height: 10),
                            Text(
                              "网格: ${analysis.grids.map((e) => e.toStringAsFixed(4)).join(" / ")}",
                              style: const TextStyle(
                                color: Color(0xFF465066),
                                fontSize: 13,
                              ),
                            ),
                          ],
                          const SizedBox(height: 10),
                          Text(
                            analysis.signalReason,
                            style: const TextStyle(
                              color: Color(0xFF1F2A44),
                              fontSize: 14,
                              height: 1.4,
                            ),
                          ),
                        ],
                      ),
                    ),
                    _sectionCard(
                      title: "板块情绪",
                      icon: Icons.hub_outlined,
                      child: Column(
                        crossAxisAlignment: CrossAxisAlignment.start,
                        children: [
                          Wrap(
                            spacing: 8,
                            runSpacing: 8,
                            children: [
                              _pill(
                                analysis.sectorName,
                                color: const Color(0xFF2457FF),
                              ),
                              _pill(
                                analysis.sectorLevel,
                                color: const Color(0xFF6C7387),
                              ),
                            ],
                          ),
                          if (analysis.sectorComment.isNotEmpty) ...[
                            const SizedBox(height: 10),
                            Text(
                              analysis.sectorComment,
                              style: const TextStyle(
                                color: Color(0xFF465066),
                                fontSize: 14,
                                height: 1.4,
                              ),
                            ),
                          ],
                        ],
                      ),
                    ),
                    _sectionCard(
                      title: "AI 结论",
                      icon: Icons.psychology_alt_outlined,
                      child: Column(
                        crossAxisAlignment: CrossAxisAlignment.start,
                        children: [
                          Row(
                            children: [
                              _pill(
                                analysis.aiAction,
                                color: _actionColor(analysis.aiAction),
                              ),
                              const SizedBox(width: 8),
                              Text(
                                analysis.latestSource,
                                style: const TextStyle(
                                  color: Color(0xFF6C7387),
                                  fontSize: 12,
                                ),
                              ),
                            ],
                          ),
                          const SizedBox(height: 10),
                          Text(
                            analysis.aiReason.isEmpty
                                ? "暂无解释"
                                : analysis.aiReason,
                            style: const TextStyle(
                              color: Color(0xFF1F2A44),
                              fontSize: 14,
                              height: 1.45,
                            ),
                          ),
                        ],
                      ),
                    ),
                  ] else ...[
                    const SizedBox(height: 48),
                    const Center(
                      child: Text(
                        "暂无分析数据，下拉重试",
                        style: TextStyle(
                          color: Color(0xFF6C7387),
                          fontSize: 14,
                        ),
                      ),
                    ),
                  ],
                ],
              ),
            ),
    );
  }
}

class _AccountInfoPage extends StatelessWidget {
  const _AccountInfoPage({
    required this.account,
    required this.holdingCount,
    required this.cashText,
  });

  final AppAccount account;
  final int holdingCount;
  final String cashText;

  String _safeText(String value) {
    final text = value.trim();
    return text.isEmpty ? "--" : text;
  }

  Widget _infoRow({
    required IconData icon,
    required String label,
    required String value,
    bool showDivider = true,
  }) {
    return Container(
      decoration: BoxDecoration(
        border: showDivider
            ? const Border(
                bottom: BorderSide(color: Color(0xFFF0F2F7), width: 1),
              )
            : null,
      ),
      padding: const EdgeInsets.fromLTRB(14, 13, 14, 13),
      child: Row(
        children: [
          Icon(icon, size: 20, color: const Color(0xFF6C7387)),
          const SizedBox(width: 10),
          Expanded(
            child: Text(
              label,
              style: const TextStyle(
                fontSize: 15,
                color: Color(0xFF1F2A44),
              ),
            ),
          ),
          Text(
            value,
            style: const TextStyle(
              fontSize: 14,
              color: Color(0xFF7B8599),
            ),
          ),
        ],
      ),
    );
  }

  Widget _actionRow({
    required IconData icon,
    required String label,
    required VoidCallback onTap,
    bool showDivider = true,
  }) {
    return Material(
      color: Colors.transparent,
      child: InkWell(
        onTap: onTap,
        child: Container(
          decoration: BoxDecoration(
            border: showDivider
                ? const Border(
                    bottom: BorderSide(color: Color(0xFFF0F2F7), width: 1),
                  )
                : null,
          ),
          padding: const EdgeInsets.fromLTRB(14, 13, 12, 13),
          child: Row(
            children: [
              Icon(icon, size: 20, color: const Color(0xFF6C7387)),
              const SizedBox(width: 10),
              Expanded(
                child: Text(
                  label,
                  style: const TextStyle(
                    fontSize: 15,
                    color: Color(0xFF1F2A44),
                  ),
                ),
              ),
              const Icon(Icons.chevron_right_rounded, color: Color(0xFFB0B7C6)),
            ],
          ),
        ),
      ),
    );
  }

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      backgroundColor: const Color(0xFFF3F5F9),
      appBar: AppBar(
        title: const Text("账户信息"),
      ),
      body: ListView(
        padding: const EdgeInsets.all(12),
        children: [
          Container(
            decoration: BoxDecoration(
              color: Colors.white,
              borderRadius: BorderRadius.circular(16),
            ),
            child: Column(
              children: [
                _infoRow(
                  icon: Icons.manage_accounts_outlined,
                  label: "账户名称",
                  value: _safeText(account.name),
                ),
                _infoRow(
                  icon: Icons.badge_outlined,
                  label: "账户 ID",
                  value: account.id.toString(),
                ),
                _infoRow(
                  icon: Icons.account_balance_wallet_outlined,
                  label: "账户现金",
                  value: cashText,
                ),
                _infoRow(
                  icon: Icons.inventory_2_outlined,
                  label: "持仓数量",
                  value: "$holdingCount",
                ),
                _infoRow(
                  icon: Icons.calendar_month_outlined,
                  label: "创建时间",
                  value: _safeText(account.createdAt),
                ),
                _infoRow(
                  icon: Icons.update_outlined,
                  label: "更新时间",
                  value: _safeText(account.updatedAt),
                  showDivider: false,
                ),
              ],
            ),
          ),
          const SizedBox(height: 12),
          Container(
            decoration: BoxDecoration(
              color: Colors.white,
              borderRadius: BorderRadius.circular(16),
            ),
            child: Column(
              children: [
                _actionRow(
                  icon: Icons.face_retouching_natural_outlined,
                  label: "上传头像",
                  onTap: () => Navigator.of(context).pop("edit_avatar"),
                  showDivider: false,
                ),
              ],
            ),
          ),
          const SizedBox(height: 16),
          FilledButton.tonal(
            onPressed: () => Navigator.of(context).pop("logout"),
            style: FilledButton.styleFrom(
              foregroundColor: Colors.red.shade700,
              minimumSize: const Size.fromHeight(46),
            ),
            child: const Text("退出账户"),
          ),
        ],
      ),
    );
  }
}
