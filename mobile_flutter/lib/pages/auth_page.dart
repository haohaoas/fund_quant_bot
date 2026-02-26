import "dart:convert";

import "package:flutter/material.dart";

import "../services/api_client.dart";

class AuthPage extends StatefulWidget {
  const AuthPage({
    super.key,
    required this.apiClient,
    required this.onAuthenticated,
  });

  final ApiClient apiClient;
  final Future<void> Function(AuthSession session) onAuthenticated;

  @override
  State<AuthPage> createState() => _AuthPageState();
}

class _AuthPageState extends State<AuthPage> {
  final _emailController = TextEditingController();
  final _passwordController = TextEditingController();
  final _confirmController = TextEditingController();

  bool _isRegister = false;
  bool _submitting = false;
  String? _errorText;

  @override
  void dispose() {
    _emailController.dispose();
    _passwordController.dispose();
    _confirmController.dispose();
    super.dispose();
  }

  Future<void> _submit() async {
    final email = _emailController.text.trim();
    final password = _passwordController.text;
    final confirm = _confirmController.text;

    if (email.isEmpty || !email.contains("@")) {
      setState(() {
        _errorText = "请输入有效邮箱";
      });
      return;
    }
    if (password.length < 6) {
      setState(() {
        _errorText = "密码至少 6 位";
      });
      return;
    }
    if (_isRegister && password != confirm) {
      setState(() {
        _errorText = "两次输入的密码不一致";
      });
      return;
    }

    setState(() {
      _submitting = true;
      _errorText = null;
    });

    try {
      final AuthSession session;
      if (_isRegister) {
        session = await widget.apiClient.registerWithEmail(
          email: email,
          password: password,
        );
      } else {
        session = await widget.apiClient.loginWithEmail(
          email: email,
          password: password,
        );
      }

      if (!mounted) {
        return;
      }
      await widget.onAuthenticated(session);
    } catch (e) {
      if (!mounted) {
        return;
      }
      setState(() {
        _errorText = _friendlyError(e);
      });
    } finally {
      if (mounted) {
        setState(() {
          _submitting = false;
        });
      }
    }
  }

  String _friendlyError(Object error) {
    if (error is ApiException) {
      try {
        final decoded = jsonDecode(error.body);
        if (decoded is Map && decoded["detail"] != null) {
          return decoded["detail"].toString();
        }
      } catch (_) {
        // ignore decode errors and fallback below
      }
      return "请求失败 (${error.statusCode})";
    }
    return "操作失败，请稍后重试";
  }

  @override
  Widget build(BuildContext context) {
    final title = _isRegister ? "邮箱注册" : "邮箱登录";

    return Scaffold(
      backgroundColor: const Color(0xFFF3F5F9),
      body: SafeArea(
        child: Center(
          child: SingleChildScrollView(
            padding: const EdgeInsets.symmetric(horizontal: 20, vertical: 24),
            child: ConstrainedBox(
              constraints: const BoxConstraints(maxWidth: 420),
              child: Card(
                elevation: 0,
                shape: RoundedRectangleBorder(
                    borderRadius: BorderRadius.circular(18)),
                child: Padding(
                  padding: const EdgeInsets.fromLTRB(20, 20, 20, 18),
                  child: Column(
                    crossAxisAlignment: CrossAxisAlignment.stretch,
                    children: [
                      const Text(
                        "养基宝",
                        textAlign: TextAlign.center,
                        style: TextStyle(
                          fontSize: 26,
                          fontWeight: FontWeight.w700,
                          color: Color(0xFF1F2B45),
                        ),
                      ),
                      const SizedBox(height: 6),
                      Text(
                        title,
                        textAlign: TextAlign.center,
                        style: const TextStyle(
                            color: Color(0xFF667085), fontSize: 14),
                      ),
                      const SizedBox(height: 18),
                      SegmentedButton<bool>(
                        segments: const [
                          ButtonSegment<bool>(value: false, label: Text("登录")),
                          ButtonSegment<bool>(value: true, label: Text("注册")),
                        ],
                        selected: <bool>{_isRegister},
                        onSelectionChanged: _submitting
                            ? null
                            : (selected) {
                                setState(() {
                                  _isRegister = selected.first;
                                  _errorText = null;
                                });
                              },
                      ),
                      const SizedBox(height: 14),
                      TextField(
                        controller: _emailController,
                        keyboardType: TextInputType.emailAddress,
                        enabled: !_submitting,
                        decoration: const InputDecoration(
                          labelText: "邮箱",
                          border: OutlineInputBorder(),
                        ),
                      ),
                      const SizedBox(height: 12),
                      TextField(
                        controller: _passwordController,
                        obscureText: true,
                        enabled: !_submitting,
                        decoration: const InputDecoration(
                          labelText: "密码",
                          border: OutlineInputBorder(),
                        ),
                      ),
                      if (_isRegister) ...[
                        const SizedBox(height: 12),
                        TextField(
                          controller: _confirmController,
                          obscureText: true,
                          enabled: !_submitting,
                          decoration: const InputDecoration(
                            labelText: "确认密码",
                            border: OutlineInputBorder(),
                          ),
                        ),
                      ],
                      if (_errorText != null) ...[
                        const SizedBox(height: 10),
                        Text(
                          _errorText!,
                          style: const TextStyle(
                              color: Color(0xFFB42318), fontSize: 13),
                        ),
                      ],
                      const SizedBox(height: 14),
                      FilledButton(
                        onPressed: _submitting ? null : _submit,
                        style: FilledButton.styleFrom(
                            minimumSize: const Size.fromHeight(46)),
                        child: _submitting
                            ? const SizedBox(
                                width: 18,
                                height: 18,
                                child:
                                    CircularProgressIndicator(strokeWidth: 2),
                              )
                            : Text(_isRegister ? "注册并登录" : "登录"),
                      ),
                    ],
                  ),
                ),
              ),
            ),
          ),
        ),
      ),
    );
  }
}
