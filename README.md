# promql2sql
将promql转发为sql

# antlr4语法文件
来源于 https://github.com/antlr/grammars-v4/tree/master/promql

# 扩充Postgresql自定义函数

```
CREATE OR REPLACE FUNCTION public.safeDiv(numerator NUMERIC, denominator NUMERIC)
RETURNS NUMERIC AS $$
BEGIN
    IF COALESCE(numerator,0)=0 or COALESCE(denominator,0)=0 or denominator = 0 THEN
        return 0;
    ELSE
        RETURN numerator / denominator;
    END IF;
END;
$$ LANGUAGE plpgsql;
```
示例：  safeDivide(2,1)=2; safeDivide(2,0)=0;

# 编译生成
pip install antlr4-tools
cd D:\tools\java\workspace\promql2sql\promql2sql\src\main\antlr
antlr4.exe -visitor -no-listener *.g4 -o ../java/com/chinatelecom/oneops/worker/query/generate -package com.chinatelecom.oneops.worker.query.generate

# 其他限制
__name__只支持精确匹配，不支持模糊匹配