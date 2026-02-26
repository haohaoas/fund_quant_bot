import "dart:async";
import "dart:convert";

import "package:flutter/material.dart";
import "package:flutter/services.dart";
import "package:image_picker/image_picker.dart";
import "package:intl/intl.dart";

import "../models/account.dart";
import "../models/market_index.dart";
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

class _HomePageState extends State<HomePage> {
  static const List<String> _indicators = <String>["今日", "5日", "10日"];
  static const List<String> _sectorTypes = <String>["行业资金流", "概念资金流", "地域资金流"];
  static const List<String> _actions = <String>["BUY", "SELL", "SIP", "REDEEM"];

  final NumberFormat _numberFormat = NumberFormat("#,##0.00", "zh_CN");

  int _tabIndex = 0;
  String _selectedIndicator = _indicators.first;
  String _selectedSectorType = _sectorTypes.first;

  PortfolioResponse? _portfolio;
  SectorFlowResponse? _sectorFlow;
  List<WatchlistItem> _watchlist = const [];
  List<MarketIndexQuote> _majorIndices = const [];
  List<AppAccount> _accounts = const [];
  int? _selectedAccountId;

  String? _portfolioError;
  String? _watchlistError;
  String? _sectorError;
  String? _indicesError;
  String? _accountsError;

  bool _loadingPortfolio = false;
  bool _loadingWatchlist = false;
  bool _loadingSector = false;
  bool _loadingIndices = false;
  bool _loadingAccounts = false;
  bool _submittingInvestment = false;
  final Set<String> _deletingPositionCodes = <String>{};

  @override
  void initState() {
    super.initState();
    _reload();
  }

  Future<void> _reload({bool forceRefresh = false}) async {
    await _loadAccounts();
    unawaited(_loadWatchlist());
    unawaited(_loadSectorFlow());
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

  Future<void> _loadAccounts() async {
    setState(() {
      _loadingAccounts = true;
      _accountsError = null;
    });

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
      });
    } catch (error) {
      if (!mounted) {
        return;
      }
      setState(() {
        _accountsError = error.toString();
      });
    } finally {
      if (mounted) {
        setState(() {
          _loadingAccounts = false;
        });
      }
    }
  }

  Future<void> _loadPortfolio({bool forceRefresh = false}) async {
    setState(() {
      _loadingPortfolio = true;
      _portfolioError = null;
    });

    try {
      final portfolio = await widget.apiClient.fetchPortfolio(
        forceRefresh: forceRefresh,
        accountId: _activeAccountId,
      );
      if (!mounted) {
        return;
      }
      setState(() {
        _portfolio = portfolio;
      });
    } catch (error) {
      if (!mounted) {
        return;
      }
      setState(() {
        _portfolioError = error.toString();
      });
    } finally {
      if (mounted) {
        setState(() {
          _loadingPortfolio = false;
        });
      }
    }
  }

  Future<void> _loadWatchlist() async {
    setState(() {
      _loadingWatchlist = true;
      _watchlistError = null;
    });

    try {
      final watchlist = await widget.apiClient.fetchWatchlist();
      if (!mounted) {
        return;
      }
      setState(() {
        _watchlist = watchlist;
      });
    } catch (error) {
      if (!mounted) {
        return;
      }
      setState(() {
        _watchlistError = error.toString();
      });
    } finally {
      if (mounted) {
        setState(() {
          _loadingWatchlist = false;
        });
      }
    }
  }

  Future<void> _loadSectorFlow() async {
    setState(() {
      _loadingSector = true;
      _sectorError = null;
    });

    try {
      final flow = await widget.apiClient.fetchSectorFlow(
        indicator: _selectedIndicator,
        sectorType: _selectedSectorType,
        topN: 20,
      );
      if (!mounted) {
        return;
      }
      setState(() {
        _sectorFlow = flow;
      });
    } catch (error) {
      if (!mounted) {
        return;
      }
      setState(() {
        _sectorError = error.toString();
      });
    } finally {
      if (mounted) {
        setState(() {
          _loadingSector = false;
        });
      }
    }
  }

  Future<void> _loadMajorIndices() async {
    setState(() {
      _loadingIndices = true;
      _indicesError = null;
    });

    try {
      final quotes = await widget.apiClient.fetchMajorIndices();
      if (!mounted) {
        return;
      }
      setState(() {
        _majorIndices = quotes;
      });
    } catch (error) {
      if (!mounted) {
        return;
      }
      setState(() {
        _indicesError = error.toString();
      });
    } finally {
      if (mounted) {
        setState(() {
          _loadingIndices = false;
        });
      }
    }
  }

  Future<_AddInvestmentDraft?> _showAddHoldingSheet() async {
    final codeController = TextEditingController();
    final amountController = TextEditingController();
    final navController = TextEditingController();

    String selectedAction = _actions.first;
    String? formError;

    final draft = await showModalBottomSheet<_AddInvestmentDraft>(
      context: context,
      isScrollControlled: true,
      builder: (sheetContext) {
        return StatefulBuilder(
          builder: (context, setSheetState) {
            void submit() {
              final code = codeController.text.trim();
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
                  TextField(
                    controller: codeController,
                    decoration: const InputDecoration(
                      labelText: "基金代码",
                      border: OutlineInputBorder(),
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
                    decoration: const InputDecoration(
                      labelText: "金额",
                      border: OutlineInputBorder(),
                      isDense: true,
                    ),
                  ),
                  const SizedBox(height: 10),
                  TextField(
                    controller: navController,
                    keyboardType:
                        const TextInputType.numberWithOptions(decimal: true),
                    decoration: const InputDecoration(
                      labelText: "净值（可选）",
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

  Color _signedColor(double value) {
    return value >= 0 ? Colors.red.shade700 : Colors.green.shade700;
  }

  String _fmt(double value) => _numberFormat.format(value);

  double _settledAccountAsset(PortfolioAccount account) {
    return account.totalAsset - account.dailyProfit;
  }

  double? _settledPositionValue(PortfolioPosition position) {
    final marketValue = position.marketValue;
    if (marketValue == null) {
      return null;
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
        sectorPct: item.sectorPct,
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
          sectorPct: null,
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
        sectorPct: current.sectorPct,
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
              child: Container(
                padding: const EdgeInsets.fromLTRB(14, 12, 14, 12),
                decoration: const BoxDecoration(
                  border: Border(bottom: BorderSide(color: Color(0xFFEDEFF5))),
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
                          Text(
                            "¥ ${_settledPositionValue(position) == null ? "--" : _fmt(_settledPositionValue(position)!)}",
                            style: const TextStyle(
                                color: Color(0xFF7B8599), fontSize: 14),
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
                    decoration: const InputDecoration(
                      labelText: "基金代码",
                      border: OutlineInputBorder(),
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

  Widget _buildControls() {
    return Card(
      child: Padding(
        padding: const EdgeInsets.all(12),
        child: Column(
          crossAxisAlignment: CrossAxisAlignment.start,
          children: [
            const Text("筛选"),
            const SizedBox(height: 8),
            Row(
              children: [
                Expanded(
                  child: DropdownButtonFormField<String>(
                    initialValue: _selectedSectorType,
                    decoration: const InputDecoration(
                      labelText: "板块类型",
                      border: OutlineInputBorder(),
                      isDense: true,
                    ),
                    items: _sectorTypes
                        .map((value) =>
                            DropdownMenuItem(value: value, child: Text(value)))
                        .toList(),
                    onChanged: _loadingSector
                        ? null
                        : (value) {
                            if (value == null) {
                              return;
                            }
                            setState(() {
                              _selectedSectorType = value;
                            });
                            unawaited(_loadSectorFlow());
                          },
                  ),
                ),
                const SizedBox(width: 12),
                Expanded(
                  child: DropdownButtonFormField<String>(
                    initialValue: _selectedIndicator,
                    decoration: const InputDecoration(
                      labelText: "周期",
                      border: OutlineInputBorder(),
                      isDense: true,
                    ),
                    items: _indicators
                        .map((value) =>
                            DropdownMenuItem(value: value, child: Text(value)))
                        .toList(),
                    onChanged: _loadingSector
                        ? null
                        : (value) {
                            if (value == null) {
                              return;
                            }
                            setState(() {
                              _selectedIndicator = value;
                            });
                            unawaited(_loadSectorFlow());
                          },
                  ),
                ),
              ],
            ),
          ],
        ),
      ),
    );
  }

  Widget _buildSectors(BuildContext context) {
    final flow = _sectorFlow;
    if (flow == null) {
      return const SizedBox.shrink();
    }

    final theme = Theme.of(context);
    return Card(
      child: Padding(
        padding: const EdgeInsets.all(12),
        child: Column(
          crossAxisAlignment: CrossAxisAlignment.start,
          children: [
            Text(
              "板块资金流 Top ${flow.items.length}",
              style: theme.textTheme.titleMedium
                  ?.copyWith(fontWeight: FontWeight.w700),
            ),
            const SizedBox(height: 10),
            for (var i = 0; i < flow.items.length; i++)
              Padding(
                padding: const EdgeInsets.only(bottom: 8),
                child: DecoratedBox(
                  decoration: BoxDecoration(
                    border: Border.all(color: Colors.black12),
                    borderRadius: BorderRadius.circular(10),
                  ),
                  child: ListTile(
                    dense: true,
                    contentPadding:
                        const EdgeInsets.symmetric(horizontal: 12, vertical: 4),
                    leading: CircleAvatar(
                      radius: 12,
                      child: Text("${i + 1}",
                          style: const TextStyle(fontSize: 11)),
                    ),
                    title: Text(flow.items[i].name),
                    subtitle: Text("涨跌幅 ${flow.items[i].changePct}"),
                    trailing: Text(
                      "${flow.items[i].mainNet >= 0 ? "+" : ""}${_fmt(flow.items[i].mainNet)} ${flow.items[i].unit}",
                      style: TextStyle(
                        color: _signedColor(flow.items[i].mainNet),
                        fontWeight: FontWeight.w700,
                      ),
                    ),
                  ),
                ),
              ),
          ],
        ),
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
                      SizedBox(width: 40),
                    ],
                  ),
                ),
                for (final item in displayItems)
                  InkWell(
                    onTap: () {
                      Navigator.of(context).push(
                        MaterialPageRoute(
                          builder: (_) => _FundAnalysisPage(
                            apiClient: widget.apiClient,
                            code: item.code,
                            name: item.displayName,
                          ),
                        ),
                      );
                    },
                    child: Container(
                      padding: const EdgeInsets.fromLTRB(12, 9, 8, 9),
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
                          SizedBox(
                            width: 40,
                            child: item.canRemove
                                ? IconButton(
                                    tooltip: "移除",
                                    onPressed: () {
                                      final src =
                                          _findWatchlistSource(item.code);
                                      if (src == null) {
                                        return;
                                      }
                                      unawaited(_onDeleteWatchlistPressed(src));
                                    },
                                    icon: const Icon(
                                        Icons.delete_outline_rounded),
                                  )
                                : const SizedBox.shrink(),
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
        _buildSectors(context),
        if (_sectorFlow != null)
          Padding(
            padding: const EdgeInsets.only(top: 8),
            child: Text(
              "源: ${_sectorFlow!.provider} | 周期: $_selectedIndicator | 类型: $_selectedSectorType",
              textAlign: TextAlign.center,
              style: Theme.of(context).textTheme.bodySmall,
            ),
          ),
        const SizedBox(height: 12),
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
        _buildMeSection(
          [
            _buildMeRow(
              icon: Icons.group_outlined,
              title: "邀请码",
              onTap: () => _showFeatureHint("邀请码"),
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
    final tabTitles = <String>["持有", "自选", "资金流", "我的"];

    Widget body;
    switch (_tabIndex) {
      case 0:
        body = _buildHoldingsTab(context);
      case 1:
        body = _buildRecommendationsTab(context);
      case 2:
        body = _buildFlowTab(context);
      case 3:
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
        appBar: _tabIndex == 0 || _tabIndex == 3
            ? null
            : AppBar(
                title: Text(tabTitles[_tabIndex]),
              ),
        body: _tabIndex == 0 || _tabIndex == 3
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

class _FundAnalysisPage extends StatefulWidget {
  const _FundAnalysisPage({
    required this.apiClient,
    required this.code,
    required this.name,
  });

  final ApiClient apiClient;
  final String code;
  final String name;

  @override
  State<_FundAnalysisPage> createState() => _FundAnalysisPageState();
}

class _FundAnalysisPageState extends State<_FundAnalysisPage> {
  FundAnalysis? _analysis;
  String? _error;
  bool _loading = false;

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
      final result = await widget.apiClient.analyzeWatchFund(
        code: widget.code,
        name: widget.name,
      );
      if (!mounted) {
        return;
      }
      setState(() {
        _analysis = result;
      });
    } catch (error) {
      if (!mounted) {
        return;
      }
      setState(() {
        _error = error.toString();
      });
    } finally {
      if (mounted) {
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
                padding: const EdgeInsets.all(12),
                children: [
                  if (_error != null)
                    Card(
                      color: const Color(0xFFFFF4F4),
                      child: Padding(
                        padding: const EdgeInsets.all(12),
                        child: Text(
                          _error!,
                          style: TextStyle(color: Colors.red.shade700),
                        ),
                      ),
                    ),
                  if (analysis != null) ...[
                    Card(
                      child: ListTile(
                        title: Text("${analysis.name} (${analysis.code})"),
                        subtitle: Text("更新时间: ${analysis.generatedAt}"),
                        trailing: Column(
                          mainAxisAlignment: MainAxisAlignment.center,
                          crossAxisAlignment: CrossAxisAlignment.end,
                          children: [
                            Text(_fmtNum(analysis.latestPrice, digits: 4)),
                            Text(
                              analysis.latestPct == null
                                  ? "--"
                                  : "${analysis.latestPct! >= 0 ? "+" : ""}${analysis.latestPct!.toStringAsFixed(2)}%",
                              style: TextStyle(
                                color: (analysis.latestPct ?? 0) >= 0
                                    ? const Color(0xFFD6464F)
                                    : const Color(0xFF1F9D61),
                                fontWeight: FontWeight.w700,
                              ),
                            ),
                          ],
                        ),
                      ),
                    ),
                    Card(
                      child: Padding(
                        padding: const EdgeInsets.all(12),
                        child: Column(
                          crossAxisAlignment: CrossAxisAlignment.start,
                          children: [
                            const Text(
                              "策略信号",
                              style: TextStyle(
                                fontSize: 16,
                                fontWeight: FontWeight.w700,
                              ),
                            ),
                            const SizedBox(height: 8),
                            Text("动作: ${analysis.signalAction}"),
                            Text("仓位建议: ${analysis.signalPositionHint}"),
                            if (analysis.basePrice != null)
                              Text(
                                  "参考中枢: ${analysis.basePrice!.toStringAsFixed(4)}"),
                            if (analysis.grids.isNotEmpty)
                              Text(
                                "网格: ${analysis.grids.map((e) => e.toStringAsFixed(4)).join(" / ")}",
                              ),
                            const SizedBox(height: 6),
                            Text(analysis.signalReason),
                          ],
                        ),
                      ),
                    ),
                    Card(
                      child: Padding(
                        padding: const EdgeInsets.all(12),
                        child: Column(
                          crossAxisAlignment: CrossAxisAlignment.start,
                          children: [
                            const Text(
                              "板块情绪",
                              style: TextStyle(
                                fontSize: 16,
                                fontWeight: FontWeight.w700,
                              ),
                            ),
                            const SizedBox(height: 8),
                            Text("板块: ${analysis.sectorName}"),
                            Text("等级: ${analysis.sectorLevel}"),
                            if (analysis.sectorComment.isNotEmpty)
                              Text(analysis.sectorComment),
                          ],
                        ),
                      ),
                    ),
                    Card(
                      child: Padding(
                        padding: const EdgeInsets.all(12),
                        child: Column(
                          crossAxisAlignment: CrossAxisAlignment.start,
                          children: [
                            const Text(
                              "AI 结论",
                              style: TextStyle(
                                fontSize: 16,
                                fontWeight: FontWeight.w700,
                              ),
                            ),
                            const SizedBox(height: 8),
                            Text("建议动作: ${analysis.aiAction}"),
                            const SizedBox(height: 4),
                            Text(analysis.aiReason.isEmpty
                                ? "暂无解释"
                                : analysis.aiReason),
                          ],
                        ),
                      ),
                    ),
                  ],
                  const SizedBox(height: 20),
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
