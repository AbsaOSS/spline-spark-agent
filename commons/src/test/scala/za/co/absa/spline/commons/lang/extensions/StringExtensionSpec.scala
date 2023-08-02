/*
 * Copyright 2023 ABSA Group Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package za.co.absa.spline.commons.lang.extensions

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import za.co.absa.spline.commons.lang.extensions.StringExtension._

import java.security.InvalidParameterException

class StringExtensionSpec extends AnyFunSuite with Matchers {
  test("StringOps.replaceChars - empty replacements") {
    val s = "supercalifragilisticexpialidocious"
    assert(s.replaceChars(Map.empty) == s)
  }

  test("StringOps.replaceChars - no hits") {
    val s = "supercalifragilisticexpialidocious"
    val map = Map('1' -> '5', '2' -> '6', '3' -> '7')
    assert(s.replaceChars(map) == s)
  }

  test("StringOps.replaceChars - replace all to same char") {
    val s: String = "abcba"
    val map = Map('a' -> 'x', 'b' -> 'x', 'c' -> 'x', 'd' -> 'x')
    assert(s.replaceChars(map) == "xxxxx")
  }

  test("StringOps.replaceChars - swap characters") {
    val s: String = "abcba"
    val map = Map('a' -> 'b', 'b' -> 'a')
    assert(s.replaceChars(map) == "bacab")
  }

  test("StringOps.findFirstUnquoted - empty string") {
    var result = "".findFirstUnquoted(Set.empty, Set.empty)
    assert(result.isEmpty)
    result = "".findFirstUnquoted(Set('a'), Set.empty)
    assert(result.isEmpty)
    result = "".findFirstUnquoted(Set('a', 'b', 'c'), Set('\''))
    assert(result.isEmpty)
    result = "".findFirstUnquoted(Set('a', 'b', 'c'), Set('\'', '"'))
    assert(result.isEmpty)
  }

  test("StringOps.findFirstUnquoted - no quotes") {
    var result = "Hello world".findFirstUnquoted(Set('x', 'y', 'z'), Set.empty)
    assert(result.isEmpty)
    result = "Hello world".findFirstUnquoted(Set('w'), Set.empty)
    assert(result.contains(6))
    result = "Hello world".findFirstUnquoted(Set('w', 'e', 'l'), Set.empty)
    assert(result.contains(1))
  }

  test("StringOps.findFirstUnquoted - simple quotes") {
    val quotes = Set('\'')
    var result = "Hello 'world'".findFirstUnquoted(Set('w'), quotes)
    assert(result.isEmpty)
    result = "Hello 'world'".findFirstUnquoted(Set('w', 'e', 'l'), quotes)
    assert(result.contains(1))
    result = "'Hello' world".findFirstUnquoted(Set('w', 'e', 'l'), quotes)
    assert(result.contains(8))
  }

  test("StringOps.findFirstUnquoted - multiple quotes") {
    val charsToFind = Set('w', 'e', 'l')
    val quotes = Set('\'', '`')
    var result = "`Hello` 'world'".findFirstUnquoted(charsToFind, quotes)
    assert(result.isEmpty)
    result = "`Hello` 'wor'ld".findFirstUnquoted(charsToFind, quotes)
    assert(result.contains(13))
    result = "`Hel'lo` 'wor'ld".findFirstUnquoted(charsToFind, quotes)
    assert(result.contains(14))
  }

  test("StringOps.findFirstUnquoted - using escape character") {
    val charsToFind = Set('w', 'e', 'l')
    val quotes = Set('\'', '`')
    var result = "`Hello` \\'world".findFirstUnquoted(charsToFind, quotes) //hasn't started
    assert(result.contains(10))
    result = "`H\\`ello` 'wor\'ld".findFirstUnquoted(charsToFind, quotes) //hasn't ended
    assert(result.contains(15))
    result = "`Hello\\`` 'wor'ld".findFirstUnquoted(charsToFind, quotes) //escaped followed by unescaped
    assert(result.contains(15))
    result = "\\ `Hello` 'world'".findFirstUnquoted(charsToFind, quotes) //escape elsewhere
    assert(result.isEmpty)
    result = "H\\e\\l\\lo \\wor\\ld'".findFirstUnquoted(charsToFind, quotes) //hits escaped
    assert(result.isEmpty)
  }

  test("StringOps.findFirstUnquoted - quote between search characters") {
    val charsToFind = Set('w', 'e', 'l', '\'')
    val quotes = Set('\'', '`')
    var result = "Hello \\'world".findFirstUnquoted(charsToFind, quotes) //simple
    assert(result.contains(1))
    result = "`Hello` \\'world".findFirstUnquoted(charsToFind, quotes) //quote hit
    assert(result.contains(9))
    result = "`Hello` 'world'".findFirstUnquoted(charsToFind, quotes) //just quotes
    assert(result.isEmpty)
    result = "`Hello\\'` 'world'".findFirstUnquoted(charsToFind, quotes) //within other quotes
    assert(result.isEmpty)
    result = "`Hello` '\\'world'".findFirstUnquoted(charsToFind, quotes) //within same quotes
    assert(result.isEmpty)
  }

  test("StringOps.findFirstUnquoted - custom escape character") {
    val charsToFind = Set('w', 'e', 'l')
    val quotes = Set('\'', '`')
    val escapeChar = '~'
    var result = "`Hello` ~'world".findFirstUnquoted(charsToFind, quotes, escapeChar) //hasn't started
    assert(result.contains(10))
    result = "`H~`ello` 'wor'ld".findFirstUnquoted(charsToFind, quotes, escapeChar) //hasn't ended
    assert(result.contains(15))
    result = "`Hello~`` 'wor'ld".findFirstUnquoted(charsToFind, quotes, escapeChar) //escaped followed by unescaped
    assert(result.contains(15))
    result = "~ `Hello` 'world'".findFirstUnquoted(charsToFind, quotes, escapeChar) //escape elsewhere
    assert(result.isEmpty)
    result = "`Hello~`` 'wor'\\ld".findFirstUnquoted(charsToFind, quotes, escapeChar) //mix-in standard escape
    assert(result.contains(16))
  }

  test("StringOps.findFirstUnquoted - many escapes") { //better to do with other then \
    val charsToFind = Set('w')
    val quotes = Set('\'')
    val escapeChar = '~'
    var result = "Hello ~~world'".findFirstUnquoted(charsToFind, quotes, escapeChar) //escaped escape -> hit valid
    assert(result.contains(8))
    result = "Hello ~~'world'".findFirstUnquoted(charsToFind, quotes, escapeChar) //escaped escape -> quotes valid
    assert(result.isEmpty)
    result = "Hello ~~~world'".findFirstUnquoted(charsToFind, quotes, escapeChar) //3x -> hit escaped
    assert(result.isEmpty)
    result = "Hello ~~~'world'".findFirstUnquoted(charsToFind, quotes, escapeChar) //3x -> quote escaped
    assert(result.contains(10))
    result = "'Hello ~~~~~'world'".findFirstUnquoted(charsToFind, quotes, escapeChar) //5x -> quote escaped, whole string quoted
    assert(result.isEmpty)
  }

  test("StringOps.findFirstUnquoted - escape in search chars") { //better to do with other then \
    val escapeChar = '~'
    val quotes = Set('\'')
    val charsToFind = Set('w', escapeChar)
    var result = "Hello ~~world'".findFirstUnquoted(charsToFind, quotes, escapeChar) //escaped escape -> hit valid
    assert(result.contains(7))
    result = "Hello '~~world'".findFirstUnquoted(charsToFind, quotes, escapeChar) //escaped escape in quotes
    assert(result.isEmpty)
    result = "Hello ~'~~world".findFirstUnquoted(charsToFind, quotes, escapeChar) //escaped quote
    assert(result.contains(9))
    result = "Hello ~world~~".findFirstUnquoted(charsToFind, quotes, escapeChar) //escaped normal hit, escaped escape follows
    assert(result.contains(13))
  }

  test("StringOps.findFirstUnquoted - escape in quote chars") { //better to do with other then \
    val escapeChar = '~'
    val quotes = Set('\'', escapeChar)
    val charsToFind = Set('w', 'e', 'l')
    var result = "~'Hello world'".findFirstUnquoted(charsToFind, quotes, escapeChar) //simple escape
    assert(result.contains(3))
    result = "~~Hello ~~pole".findFirstUnquoted(charsToFind, quotes, escapeChar) //escape as quotes
    assert(result.contains(12))
    result = "~~Hello ~~'pole'".findFirstUnquoted(charsToFind, quotes, escapeChar) //escape as quotes followed by standard quotes
    assert(result.isEmpty)
    result = "~~Hello ~~world".findFirstUnquoted(charsToFind, quotes, escapeChar) //escape as quotes directly followed by hit
    assert(result.contains(10))
    result = "~~Hello ~~~world".findFirstUnquoted(charsToFind, quotes, escapeChar) //escape as quotes and right after escaped hit
    assert(result.contains(14))
  }

  test("StringOps.findFirstUnquoted - escape in search and quote chars") { //better to do with other then \
    val escapeChar = '!'
    val quotes = Set('\'', escapeChar)
    val charsToFind = Set('w', 'e', 'l', escapeChar)
    val expectedMessage = s"Escape character '$escapeChar 'is both between charsToFind and quoteChars. That's not allowed."
    val caught = intercept[InvalidParameterException] {
      "All the jewels of the world!".findFirstUnquoted(charsToFind, quotes, escapeChar)
    }
    assert(caught.getMessage == expectedMessage)
  }

  test("StringOps.hasUnquoted") {
    assert(!"".hasUnquoted(Set.empty, Set.empty))
    assert(!"Hello world".hasUnquoted(Set('x'), Set.empty))
    assert("Hello world".hasUnquoted(Set('w', 'e', 'l'), Set('`')))
    assert(!"`Hello world`".hasUnquoted(Set('w', 'e', 'l'), Set('`')))
  }

  test("StringOps.countUnquoted: empty variants") {
    val expected = Map(
      'x'->0,
      'y'->0,
      'z'->0
    )
    val charsToFind = Set('x', 'y', 'z')
    val empty = Set.empty[Char]
    assert("".countUnquoted(charsToFind, Set('"')) == expected)
    assert("Lorem ipsum".countUnquoted(charsToFind, empty) == expected)
    assert("Hello world".countUnquoted(empty, Set('|')) == Map.empty)
  }

  test("StringOps.countUnquoted: simple test") {
    val charsToFind = Set('x', 'y', 'z')
    val expected1 = Map(
      'x'->0,
      'y'->0,
      'z'->0
    )
    assert("Lorem ipsum".countUnquoted(charsToFind, Set('\'')) == expected1)
    assert("Hello 'xyz' world".countUnquoted(charsToFind, Set('\'')) == expected1)
    val expected2 = Map(
      'x'->3,
      'y'->2,
      'z'->1
    )
    assert("xxxyzy".countUnquoted(charsToFind, Set('-')) == expected2)
    val expected3 = Map(
      'x'->1,
      'y'->2,
      'z'->5
    )
    assert("x-xxy-yyzzz|zyyy|zz".countUnquoted(charsToFind, Set('-', '|')) == expected3)
  }

  test("StringOps.countUnquoted: escape involved") {
    val charsToFind = Set('x', 'y', 'z')
    val expected = Map(
      'x'->3,
      'y'->2,
      'z'->3
    )
    assert("x~yz~'xxyyzz 'xxxx~'zzzz'~''yyyy".countUnquoted(charsToFind, Set('\''),'~') == expected)
  }

  test("StringOps.countUnquoted: search and quote chars overlap") {
    val charsToFind = Set('a', '#', '$', '%')
    val quoteChars = Set('$', '%', '^')
    val expected = Map(
      'a'->0,
      '#'->1,
      '$'->1,
      '%'->0
    )
    assert("#^##^|$%%%%".countUnquoted(charsToFind, quoteChars, '|') == expected)
  }

  test("StringOps.countUnquoted: escape in search for chars") {
    val charsToFind = Set('a', 'b', 'c', 'd', '|')
    val quoteChars = Set('%', '^')
    val expected = Map(
      'a'->2,
      'b'->0,
      'c'->2,
      'd'->0,
      '|'->1
    )
    assert("aa||%bb%|^cc^a|cd||d|^b^".countUnquoted(charsToFind, quoteChars, '|') == expected)
  }

  test("StringOps.countUnquoted: escape in quote chars") {
    val charsToFind = Set('a', 'b', 'c')
    val quoteChars = Set('$', '%', '^')
    val expected = Map(
      'a'->1,
      'b'->0,
      'c'->0
    )
    assert("a$$bc$$".countUnquoted(charsToFind, quoteChars, '$') == expected)
  }

  test("StringConcatenationOps.joinWithSingleSeparator: string joining general") {
    "abc#".joinWithSingleSeparator("#def", "#") shouldBe "abc#def"
    "abc###".joinWithSingleSeparator("def", "#") shouldBe "abc###def"
    "abcSEP".joinWithSingleSeparator("def", "SEP") shouldBe "abcSEPdef"
    "abcSEPSEP".joinWithSingleSeparator("SEPSEPdef", "SEP") shouldBe "abcSEPSEPSEPdef"
  }

  test("StringConcatenationOps./: string joining with /") {
    "abc" / "123" shouldBe "abc/123"
    "aaa/" / "123" shouldBe "aaa/123"
    "bbb" / "/123" shouldBe "bbb/123"
    "ccc/" / "/123" shouldBe "ccc/123"
    "file:///" / "path" shouldBe "file:///path"
  }

  test("StringOps.nonEmptyOrElse") {
    "a".nonEmptyOrElse("b") shouldBe "a"
    "".nonEmptyOrElse("b") shouldBe "b"
    "a".nonEmptyOrElse("") shouldBe "a"
  }

  test("StringOps.coalesce") {
    "".coalesce() shouldBe ""
    "".coalesce("A", "") shouldBe "A"
    "".coalesce("", "", "B", "", "C") shouldBe "B"
    "X".coalesce("Y", "Z") shouldBe "X"
    "X".coalesce("") shouldBe "X"
  }

  test("StringOps.nonBlankOption - for blank string") {
    (null: String).nonBlankOption should be(None)
    "            ".nonBlankOption should be(None)
  }

  test("StringOps.nonBlankOption - for non blank string") {
    " foo bar 42 ".nonBlankOption should be(Some(" foo bar 42 "))
    "     .".nonBlankOption should be(Some("     ."))
    ".      ".nonBlankOption should be(Some(".      "))
    "     .      ".nonBlankOption should be(Some("     .      "))
  }

}

