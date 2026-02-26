import "package:flutter/material.dart";
import "package:flutter_test/flutter_test.dart";

void main() {
  testWidgets("renders a basic shell", (WidgetTester tester) async {
    await tester.pumpWidget(
      const MaterialApp(
        home: Scaffold(
          body: Text("Fund Quant Mobile"),
        ),
      ),
    );

    expect(find.text("Fund Quant Mobile"), findsOneWidget);
  });
}
