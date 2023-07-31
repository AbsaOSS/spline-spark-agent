/*
 * Copyright 2020 ABSA Group Limited
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

package za.co.absa.commons.lang.extensions

import java.security.InvalidParameterException
import scala.annotation.tailrec
import scala.collection.mutable

object StringExtension {

  implicit class StringOps(val string: String) extends AnyVal {

    /**
     * Replaces all occurrences of the provided characters with their mapped values
     *
     * @param replacements the map of replacements where key's are chars to search for and values are their replacements
     * @return a string with characters replaced
     */
    def replaceChars(replacements: Map[Char, Char]): String = {
      if (replacements.isEmpty) {
        string
      } else {
        val result = new mutable.StringBuilder(string.length)
        string.foreach(char => result.append(replacements.getOrElse(char, char)))
        result.toString
      }
    }

    /**
     * Function to find the first occurrence of any of the characters from the charsToFind in the string. The
     * occurrence is not considered if the character is part of a sequence within a pair of quote characters specified
     * by quoteChars param.
     * Escape character in front of a quote character will cancel its "quoting" function.
     * Escape character in front of a searched-for character will not result in positive identification of a find
     * Double escape character is considered to be escape character itself, without its special meaning
     * The escape character can be part of the charsToFind set or quoteChars set and the function will work as
     * expected (e.g. double escape character being recognized as a searched-for character or quote character), but it
     * cannot be both - that will fire an exception.
     *
     * @param charsToFind set of characters to look for
     * @param quoteChars  set of characters that are considered as quotes, everything within two (same) quote characters
     *                    is not considered
     * @param escape      the special character to escape the expected behavior within string
     * @return the index of the first find within the string, or None in case of no find
     */
    def findFirstUnquoted(charsToFind: Set[Char], quoteChars: Set[Char], escape: Char = '\\'): Option[Integer] = {
      @tailrec
      def scan(sub: String, idx: Integer, charToExitQuotes: Option[Char], escaped: Boolean = false): Option[Integer] = {
        // escaped flag defaults to false, as every non-escape character clears it
        val head = sub.headOption
        (head, examineChar(head, charsToFind, quoteChars, escape, charToExitQuotes, escaped)) match {
          case (None, _) => None // scanned the whole string without a hit
          case (_, None) => Option(idx) // hit found
          case (_, Some((nextCharToExitQuotes, nextEscaped))) =>
            scan(sub.tail, idx + 1, nextCharToExitQuotes, nextEscaped) // continue search
        }
      }

      checkInputsOverlap(charsToFind, quoteChars, escape: Char)
      scan(string, 0, charToExitQuotes = None)
    }

    /**
     * Function to check if any of the characters from the charsToFind occurs in the string.
     * The occurrence is not considered if the character is part of a sequence within a pair of quote characters
     * specified by quoteChars param.
     * Escape character in front of a quote character will cancel its "quoting" function.
     * Escape character in front of a searched-for character will not result in positive identification of a find
     * Double escape character is considered to be escape character itself, without its special meaning
     * The escape character can be part of the charsToFind set or quoteChars set and the function will work as
     * expected (e.g. double escape character being recognized as a searched-for character or quote character), but it
     * cannot be both - that will fire an exception.
     *
     * @param charsToFind set of characters to look for
     * @param quoteChars  set of characters that are considered as quotes, everything within two (same) quote characters
     *                    is not considered
     * @param escape      the special character to escape the expected behavior within string
     * @return true if anything is found, false otherwise
     */
    def hasUnquoted(charsToFind: Set[Char], quoteChars: Set[Char], escape: Char = '\\'): Boolean = {
      findFirstUnquoted(charsToFind, quoteChars, escape).nonEmpty
    }

    /**
     * Counts the occurrences of the chars to find. The occurrence is not considered if the character is part of a
     * sequence within a pair of quote characters specified by quoteChars param.
     * Escape character in front of a quote character will cancel its "quoting" function.
     * Escape character in front of a searched-for character will not result in positive identification of a find
     * Double escape character is considered to be escape character itself, without its special meaning
     * The escape character can be part of the charsToFind set or quoteChars set and the function will work as
     * expected (e.g. double escape character being recognized as a searched-for character or quote character), but it
     * cannot be both - that will fire an exception.
     *
     * @param charsToFind set of characters to look for
     * @param quoteChars  set of characters that are considered as quotes, everything within two (same) quote characters
     *                    is not considered
     * @param escape      the special character to escape the expected behavior within string
     * @return map where charsToFind are the keys and values are the respective number of occurrences
     */
    def countUnquoted(charsToFind: Set[Char], quoteChars: Set[Char], escape: Char = '\\'): Map[Char, Int] = {
      checkInputsOverlap(charsToFind, quoteChars, escape: Char)
      val resultInit: Map[Char, Int] = charsToFind.map((_, 0)).toMap
      val examineInit: (Option[Char], Boolean) = (Option.empty, false)
      val (result, _) = string.foldLeft((resultInit, examineInit))((acc, char) => {
        val (resultAcc, (charToExitQuotes, escaped)) = acc
        val examineResult = examineChar(Option(char), charsToFind, quoteChars, escape, charToExitQuotes, escaped)
        examineResult.map((resultAcc, _)) //no hit, propagate the examineResult
          .getOrElse(resultAcc ++ Map(char -> (resultAcc(char) + 1)), examineInit)
      })
      result
    }

    /**
     * Function to check if string is empty, if that is the case it returns default, otherwise the string itself.
     *
     * @param default string to use in case of emptiness
     * @return default if string is nonempty, the string otherwise
     */
    def nonEmptyOrElse(default: => String): String = {
      if (string.isEmpty) default else string
    }

    /**
     * Gets first nonempty string from collection consisting of this string and alternatives.
     * This string has the highest priority, then the leftmost alternative, then next alternative to the right, etc.
     *
     * @example "".coalesce("abc", "cde")     // -> "abc"
     * @example "123".coalesce("abc", "cde")  // -> "123"
     * @example "".coalesce("", "cde", "efg") // -> "cde"
     * @param alternatives strings that are used in case of emptiness
     * @return first nonempty string (if there is any)
     */
    def coalesce(alternatives: String*): String = {
      alternatives.foldLeft(string)(_.nonEmptyOrElse(_))
    }

    /**
     * Converts string into Option by returning
     * Some(string) in case string is not null and contains any non-whitespace characters, None otherwise.
     *
     * @return Option containing string if its is non blank
     */
    def nonBlankOption: Option[String] =
      if (string == null) None
      else if (string.trim.isEmpty) None
      else Some(string)

    /**
     * Investigates if the character in the relation to previous characters and charsToFind
     *
     * @param char             character to examine, for easier matching and also support end of string, it's an Option
     * @param charsToFind      set of characters to look for
     * @param quoteChars       set of characters that are considered as quotes, everything within two (same) quote characters
     *                         is not considered
     * @param escape           the special character to escape the expected behavior within string
     * @param charToExitQuotes character that would is awaited to exit the "quotes"; if not empty means scan is within
     *                         "quotes"
     * @param escaped          if true the previous character was the escape character
     * @return Optional 2-tuple, None means hit (char is one of the charsToFind), otherwise it's the value of
     *         charToExitQuotes and escaped for the next character
     */
    private def examineChar(char: Option[Char],
                            charsToFind: Set[Char],
                            quoteChars: Set[Char],
                            escape: Char,
                            charToExitQuotes: Option[Char],
                            escaped: Boolean = false): Option[(Option[Char], Boolean)] = {
      (char, escaped) match {
        // no more chars on input probably
        case (None, _) => Option(None, false)
        // following cases are to address situations when the char character is within quotes (not yet closed)
        // exit quote unless it's escaped or is the escape character itself
        case (`charToExitQuotes`, false) if !charToExitQuotes.contains(escape) => Option(None, false)
        // escaped exit quote means exit only if it's the escape character itself
        case (`charToExitQuotes`, true) if charToExitQuotes.contains(escape) => Option(None, false)
        // escape charter found (not necessary withing quotes, but has to be handled it this order)
        case (Some(`escape`), false) => Option(charToExitQuotes, true)
        // any other character within quotes, no special case
        case _ if charToExitQuotes.nonEmpty => Option(charToExitQuotes, false)
        // following cases addresses situations when the char character is outside quotes
        // escaped escape character if it's also a quote character
        case (Some(`escape`), true) if quoteChars.contains(escape) => Option(Option(escape), false)
        //escaped escape character if it's also a character to find
        case (Some(`escape`), true) if charsToFind.contains(escape) => None
        // entering quotes
        case (Some(c), false) if quoteChars.contains(c) => Option(Option(c), false)
        // found one of the characters to search for
        case (Some(c), false) if charsToFind.contains(c) => None
        // an escaped quote character that is also within the characters to find
        case (Some(c), true) if quoteChars.contains(c) && charsToFind.contains(c) => None
        // all other cases, continue scan
        case _ => Option(None, false)
      }
    }

    private def checkInputsOverlap(charsToFind: Set[Char], quoteChars: Set[Char], escape: Char = '\\'): Unit = {
      if (charsToFind.contains(escape) && quoteChars.contains(escape)) {
        throw new InvalidParameterException(
          s"Escape character '$escape 'is both between charsToFind and quoteChars. That's not allowed."
        )
      }
    }

  }

  implicit class StringConcatenationOps(val string: String) extends AnyVal {

    /**
     * Joins two strings with / while stripping single existing trailing/leading "/" in between:
     * {{{
     * "abc" / "123" -> "abc/123"
     * "abc/" / "123" -> "abc/123"
     * "abc" / "/123" -> "abc/123"
     * "abc/" / "/123" -> "abc/123",
     * but:
     * "file:///" / "path" -> "file:///path",
     * }}}
     *
     * @param another the second string we are appending after the `/` separator
     * @return this/another (this has stripped trailing / if present, another has leading / stripped if present)
     */
    def /(another: String): String = {
      joinWithSingleSeparator(another, "/")
    }

    private[lang] def joinWithSingleSeparator(another: String, sep: String): String = {
      val sb = new mutable.StringBuilder
      sb.append(string.stripSuffix(sep))
      sb.append(sep)
      sb.append(another.stripPrefix(sep))
      sb.mkString
    }

  }

}
