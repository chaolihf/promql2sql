# promql2sql
将promql转发为sql

# antlr4语法文件
来源于 https://github.com/antlr/grammars-v4/tree/master/promql

# 编译生成
pip install antlr4-tools
cd D:\tools\java\workspace\promql2sql\promql2sql\src\main\antlr
antlr4.exe -visitor -no-listener *.g4 -o ../java/com/chinatelecom/oneops/worker/query/generate -package com.chinatelecom.oneops.worker.query.generate