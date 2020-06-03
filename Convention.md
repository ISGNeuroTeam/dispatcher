# Coding Convention

## Что это

Здесь описаны стиль и соглашения по написанию транслятора команд из OTL в Spark

## Реализация команд

1. В случае каких-то эксепшенов метод `transform` возвращает исходный датафрейм (`_df`).
2. Часто делается так:
```scala
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._
...
val df = _df.withColumn("rows", explode(col("temp")))
```
Не очень понятно, откуда вообще взялись explode и col: из импорта, из базового класса, из какой-то другой функции... Предлагается с импортами делать так:
```scala
import org.apache.spark.sql.{ functions => F }
...
val df = _df.withColumn("rows", F.explode(F.col("temp")))
```
Или даже так:
```scala
import org.apache.spark.sql.{ functions => F }
import spark.implicits._
...
val df = _df.withColumn("rows", F.explode($"temp"))
```
чтобы не тащить везде `F.col` (а это требуется много где).

## Scala best practices

1. При работе с последовательностями избегать конструкций `seqName(0)` или `seqName.head`. Это выкинет эксепшен, если последовательность пустая. Лучше делать так:
```scala
val obj = seqName.headOption match {
  case Some(_obj) => _obj
  case _ => return _df // или какой-то другой обработчик
}
```
Можно сразу внутри `match` писать функции:
```scala
seqName.headOption match {
  case Some(obj) => {
    // do something, calculate df
    df
  }
  case _ => _df
}
```

Если надо использовать `head` и `tail` (например, в конструкции `df.select(cols.head, cols.tail:_*)`), то лучше делать так:
```scala
val res = cols match {
  case head :: tail => df.select(head, tail:_*)
  case _ => df
}
```
2. При работе с типом `Map` избегать конструкций `mapName(key)`. Если такого ключа нет, то вернется эксепшен. Лучше делать так:
```scala
val value = mapName.getOrElse(key, defaultValue)
```
или даже
```scala
val value = mapName.getOrElse(key, return _df) // если этот ключ жизненно важен для transform и без него ничего не работает
```
