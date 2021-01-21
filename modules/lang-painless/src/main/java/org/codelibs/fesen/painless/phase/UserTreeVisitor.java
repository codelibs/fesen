/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.codelibs.fesen.painless.phase;

import org.codelibs.fesen.painless.node.EAssignment;
import org.codelibs.fesen.painless.node.EBinary;
import org.codelibs.fesen.painless.node.EBooleanComp;
import org.codelibs.fesen.painless.node.EBooleanConstant;
import org.codelibs.fesen.painless.node.EBrace;
import org.codelibs.fesen.painless.node.ECall;
import org.codelibs.fesen.painless.node.ECallLocal;
import org.codelibs.fesen.painless.node.EComp;
import org.codelibs.fesen.painless.node.EConditional;
import org.codelibs.fesen.painless.node.EDecimal;
import org.codelibs.fesen.painless.node.EDot;
import org.codelibs.fesen.painless.node.EElvis;
import org.codelibs.fesen.painless.node.EExplicit;
import org.codelibs.fesen.painless.node.EFunctionRef;
import org.codelibs.fesen.painless.node.EInstanceof;
import org.codelibs.fesen.painless.node.ELambda;
import org.codelibs.fesen.painless.node.EListInit;
import org.codelibs.fesen.painless.node.EMapInit;
import org.codelibs.fesen.painless.node.ENewArray;
import org.codelibs.fesen.painless.node.ENewArrayFunctionRef;
import org.codelibs.fesen.painless.node.ENewObj;
import org.codelibs.fesen.painless.node.ENull;
import org.codelibs.fesen.painless.node.ENumeric;
import org.codelibs.fesen.painless.node.ERegex;
import org.codelibs.fesen.painless.node.EString;
import org.codelibs.fesen.painless.node.ESymbol;
import org.codelibs.fesen.painless.node.EUnary;
import org.codelibs.fesen.painless.node.SBlock;
import org.codelibs.fesen.painless.node.SBreak;
import org.codelibs.fesen.painless.node.SCatch;
import org.codelibs.fesen.painless.node.SClass;
import org.codelibs.fesen.painless.node.SContinue;
import org.codelibs.fesen.painless.node.SDeclBlock;
import org.codelibs.fesen.painless.node.SDeclaration;
import org.codelibs.fesen.painless.node.SDo;
import org.codelibs.fesen.painless.node.SEach;
import org.codelibs.fesen.painless.node.SExpression;
import org.codelibs.fesen.painless.node.SFor;
import org.codelibs.fesen.painless.node.SFunction;
import org.codelibs.fesen.painless.node.SIf;
import org.codelibs.fesen.painless.node.SIfElse;
import org.codelibs.fesen.painless.node.SReturn;
import org.codelibs.fesen.painless.node.SThrow;
import org.codelibs.fesen.painless.node.STry;
import org.codelibs.fesen.painless.node.SWhile;

public interface UserTreeVisitor<Scope> {

    void visitClass(SClass userClassNode, Scope scope);
    void visitFunction(SFunction userFunctionNode, Scope scope);

    void visitBlock(SBlock userBlockNode, Scope scope);
    void visitIf(SIf userIfNode, Scope scope);
    void visitIfElse(SIfElse userIfElseNode, Scope scope);
    void visitWhile(SWhile userWhileNode, Scope scope);
    void visitDo(SDo userDoNode, Scope scope);
    void visitFor(SFor userForNode, Scope scope);
    void visitEach(SEach userEachNode, Scope scope);
    void visitDeclBlock(SDeclBlock userDeclBlockNode, Scope scope);
    void visitDeclaration(SDeclaration userDeclarationNode, Scope scope);
    void visitReturn(SReturn userReturnNode, Scope scope);
    void visitExpression(SExpression userExpressionNode, Scope scope);
    void visitTry(STry userTryNode, Scope scope);
    void visitCatch(SCatch userCatchNode, Scope scope);
    void visitThrow(SThrow userThrowNode, Scope scope);
    void visitContinue(SContinue userContinueNode, Scope scope);
    void visitBreak(SBreak userBreakNode, Scope scope);

    void visitAssignment(EAssignment userAssignmentNode, Scope scope);
    void visitUnary(EUnary userUnaryNode, Scope scope);
    void visitBinary(EBinary userBinaryNode, Scope scope);
    void visitBooleanComp(EBooleanComp userBooleanCompNode, Scope scope);
    void visitComp(EComp userCompNode, Scope scope);
    void visitExplicit(EExplicit userExplicitNode, Scope scope);
    void visitInstanceof(EInstanceof userInstanceofNode, Scope scope);
    void visitConditional(EConditional userConditionalNode, Scope scope);
    void visitElvis(EElvis userElvisNode, Scope scope);
    void visitListInit(EListInit userListInitNode, Scope scope);
    void visitMapInit(EMapInit userMapInitNode, Scope scope);
    void visitNewArray(ENewArray userNewArrayNode, Scope scope);
    void visitNewObj(ENewObj userNewObjectNode, Scope scope);
    void visitCallLocal(ECallLocal userCallLocalNode, Scope scope);
    void visitBooleanConstant(EBooleanConstant userBooleanConstantNode, Scope scope);
    void visitNumeric(ENumeric userNumericNode, Scope scope);
    void visitDecimal(EDecimal userDecimalNode, Scope scope);
    void visitString(EString userStringNode, Scope scope);
    void visitNull(ENull userNullNode, Scope scope);
    void visitRegex(ERegex userRegexNode, Scope scope);
    void visitLambda(ELambda userLambdaNode, Scope scope);
    void visitFunctionRef(EFunctionRef userFunctionRefNode, Scope scope);
    void visitNewArrayFunctionRef(ENewArrayFunctionRef userNewArrayFunctionRefNode, Scope scope);
    void visitSymbol(ESymbol userSymbolNode, Scope scope);
    void visitDot(EDot userDotNode, Scope scope);
    void visitBrace(EBrace userBraceNode, Scope scope);
    void visitCall(ECall userCallNode, Scope scope);
}
