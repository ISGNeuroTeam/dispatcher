package ot.scalaotl
package utils
package searchinternals

import net.totietje.evaluator.Token._
import net.totietje.evaluator.Associativity

object BooleanToken {
  object And extends Operator[Boolean](0, Associativity.Left) {
    override def apply(left: Boolean, right: Boolean): Boolean = left & right
  }

  object Or extends Operator[Boolean](1, Associativity.Left) {
    override def apply(left: Boolean, right: Boolean): Boolean = left | right
  }

  object Not extends Function[Boolean](1) {
    //args will have length 1
    override def apply(args: Seq[Boolean]): Boolean = !args.head
  }

  object True extends Value[Boolean] {
    override def apply(): Boolean = true
  }

  object False extends Value[Boolean] {
    override def apply(): Boolean = false
  }
}

import BooleanToken._
import net.totietje.evaluator.Token._
import net.totietje.evaluator.{ EvaluationException, AbstractTokenizer, Token }

object BooleanTokenizer extends AbstractTokenizer[Boolean] {
  override protected def parseAfterValueChar(op: Char): Option[Token[Boolean]] = op match {
    case '&' => Some(And)
    case '|' => Some(Or)
    case ')' => Some(CloseParen()) //CloseParen case class provided by Token
    case _   => None
  }

  override protected def parseOtherChar(char: Char): Option[Token[Boolean]] = char match {
    case '!' => Some(Not)
    case '(' => Some(OpenParen()) //OpenParen case class provided by Token
    case _   => None
  }

  override protected def parseWord(word: String): Token[Boolean] = word.toLowerCase match {
    case "true"  => True
    case "false" => False
    case _       => throw EvaluationException(s"Unrecognised word $word")
  }
}

//Finally, we can create our BooleanEvaluator:
import net.totietje.evaluator.AbstractEvaluator

object BooleanEvaluator extends AbstractEvaluator[Boolean] {
  def tokenizer = BooleanTokenizer
}
