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

import org.codelibs.fesen.painless.ir.BinaryImplNode;
import org.codelibs.fesen.painless.ir.BinaryMathNode;
import org.codelibs.fesen.painless.ir.BlockNode;
import org.codelibs.fesen.painless.ir.BooleanNode;
import org.codelibs.fesen.painless.ir.BreakNode;
import org.codelibs.fesen.painless.ir.CastNode;
import org.codelibs.fesen.painless.ir.CatchNode;
import org.codelibs.fesen.painless.ir.ClassNode;
import org.codelibs.fesen.painless.ir.ComparisonNode;
import org.codelibs.fesen.painless.ir.ConditionalNode;
import org.codelibs.fesen.painless.ir.ConstantNode;
import org.codelibs.fesen.painless.ir.ContinueNode;
import org.codelibs.fesen.painless.ir.DeclarationBlockNode;
import org.codelibs.fesen.painless.ir.DeclarationNode;
import org.codelibs.fesen.painless.ir.DefInterfaceReferenceNode;
import org.codelibs.fesen.painless.ir.DoWhileLoopNode;
import org.codelibs.fesen.painless.ir.DupNode;
import org.codelibs.fesen.painless.ir.ElvisNode;
import org.codelibs.fesen.painless.ir.FieldNode;
import org.codelibs.fesen.painless.ir.FlipArrayIndexNode;
import org.codelibs.fesen.painless.ir.FlipCollectionIndexNode;
import org.codelibs.fesen.painless.ir.FlipDefIndexNode;
import org.codelibs.fesen.painless.ir.ForEachLoopNode;
import org.codelibs.fesen.painless.ir.ForEachSubArrayNode;
import org.codelibs.fesen.painless.ir.ForEachSubIterableNode;
import org.codelibs.fesen.painless.ir.ForLoopNode;
import org.codelibs.fesen.painless.ir.FunctionNode;
import org.codelibs.fesen.painless.ir.IfElseNode;
import org.codelibs.fesen.painless.ir.IfNode;
import org.codelibs.fesen.painless.ir.InstanceofNode;
import org.codelibs.fesen.painless.ir.InvokeCallDefNode;
import org.codelibs.fesen.painless.ir.InvokeCallMemberNode;
import org.codelibs.fesen.painless.ir.InvokeCallNode;
import org.codelibs.fesen.painless.ir.ListInitializationNode;
import org.codelibs.fesen.painless.ir.LoadBraceDefNode;
import org.codelibs.fesen.painless.ir.LoadBraceNode;
import org.codelibs.fesen.painless.ir.LoadDotArrayLengthNode;
import org.codelibs.fesen.painless.ir.LoadDotDefNode;
import org.codelibs.fesen.painless.ir.LoadDotNode;
import org.codelibs.fesen.painless.ir.LoadDotShortcutNode;
import org.codelibs.fesen.painless.ir.LoadFieldMemberNode;
import org.codelibs.fesen.painless.ir.LoadListShortcutNode;
import org.codelibs.fesen.painless.ir.LoadMapShortcutNode;
import org.codelibs.fesen.painless.ir.LoadVariableNode;
import org.codelibs.fesen.painless.ir.MapInitializationNode;
import org.codelibs.fesen.painless.ir.NewArrayNode;
import org.codelibs.fesen.painless.ir.NewObjectNode;
import org.codelibs.fesen.painless.ir.NullNode;
import org.codelibs.fesen.painless.ir.NullSafeSubNode;
import org.codelibs.fesen.painless.ir.ReturnNode;
import org.codelibs.fesen.painless.ir.StatementExpressionNode;
import org.codelibs.fesen.painless.ir.StaticNode;
import org.codelibs.fesen.painless.ir.StoreBraceDefNode;
import org.codelibs.fesen.painless.ir.StoreBraceNode;
import org.codelibs.fesen.painless.ir.StoreDotDefNode;
import org.codelibs.fesen.painless.ir.StoreDotNode;
import org.codelibs.fesen.painless.ir.StoreDotShortcutNode;
import org.codelibs.fesen.painless.ir.StoreFieldMemberNode;
import org.codelibs.fesen.painless.ir.StoreListShortcutNode;
import org.codelibs.fesen.painless.ir.StoreMapShortcutNode;
import org.codelibs.fesen.painless.ir.StoreVariableNode;
import org.codelibs.fesen.painless.ir.StringConcatenationNode;
import org.codelibs.fesen.painless.ir.ThrowNode;
import org.codelibs.fesen.painless.ir.TryNode;
import org.codelibs.fesen.painless.ir.TypedCaptureReferenceNode;
import org.codelibs.fesen.painless.ir.TypedInterfaceReferenceNode;
import org.codelibs.fesen.painless.ir.UnaryMathNode;
import org.codelibs.fesen.painless.ir.WhileLoopNode;

public class IRTreeBaseVisitor<Scope> implements IRTreeVisitor<Scope> {

    @Override
    public void visitClass(ClassNode irClassNode, Scope scope) {
        irClassNode.visitChildren(this, scope);
    }

    @Override
    public void visitFunction(FunctionNode irFunctionNode, Scope scope) {
        irFunctionNode.visitChildren(this, scope);
    }

    @Override
    public void visitField(FieldNode irFieldNode, Scope scope) {
        irFieldNode.visitChildren(this, scope);
    }

    @Override
    public void visitBlock(BlockNode irBlockNode, Scope scope) {
        irBlockNode.visitChildren(this, scope);
    }

    @Override
    public void visitIf(IfNode irIfNode, Scope scope) {
        irIfNode.visitChildren(this, scope);
    }

    @Override
    public void visitIfElse(IfElseNode irIfElseNode, Scope scope) {
        irIfElseNode.visitChildren(this, scope);
    }

    @Override
    public void visitWhileLoop(WhileLoopNode irWhileLoopNode, Scope scope) {
        irWhileLoopNode.visitChildren(this, scope);
    }

    @Override
    public void visitDoWhileLoop(DoWhileLoopNode irDoWhileLoopNode, Scope scope) {
        irDoWhileLoopNode.visitChildren(this, scope);
    }

    @Override
    public void visitForLoop(ForLoopNode irForLoopNode, Scope scope) {
        irForLoopNode.visitChildren(this, scope);
    }

    @Override
    public void visitForEachLoop(ForEachLoopNode irForEachLoopNode, Scope scope) {
        irForEachLoopNode.visitChildren(this, scope);
    }

    @Override
    public void visitForEachSubArrayLoop(ForEachSubArrayNode irForEachSubArrayNode, Scope scope) {
        irForEachSubArrayNode.visitChildren(this, scope);
    }

    @Override
    public void visitForEachSubIterableLoop(ForEachSubIterableNode irForEachSubIterableNode, Scope scope) {
        irForEachSubIterableNode.visitChildren(this, scope);
    }

    @Override
    public void visitDeclarationBlock(DeclarationBlockNode irDeclarationBlockNode, Scope scope) {
        irDeclarationBlockNode.visitChildren(this, scope);
    }

    @Override
    public void visitDeclaration(DeclarationNode irDeclarationNode, Scope scope) {
        irDeclarationNode.visitChildren(this, scope);
    }

    @Override
    public void visitReturn(ReturnNode irReturnNode, Scope scope) {
        irReturnNode.visitChildren(this, scope);
    }

    @Override
    public void visitStatementExpression(StatementExpressionNode irStatementExpressionNode, Scope scope) {
        irStatementExpressionNode.visitChildren(this, scope);
    }

    @Override
    public void visitTry(TryNode irTryNode, Scope scope) {
        irTryNode.visitChildren(this, scope);
    }

    @Override
    public void visitCatch(CatchNode irCatchNode, Scope scope) {
        irCatchNode.visitChildren(this, scope);
    }

    @Override
    public void visitThrow(ThrowNode irThrowNode, Scope scope) {
        irThrowNode.visitChildren(this, scope);
    }

    @Override
    public void visitContinue(ContinueNode irContinueNode, Scope scope) {
        irContinueNode.visitChildren(this, scope);
    }

    @Override
    public void visitBreak(BreakNode irBreakNode, Scope scope) {
        irBreakNode.visitChildren(this, scope);
    }

    @Override
    public void visitBinaryImpl(BinaryImplNode irBinaryImplNode, Scope scope) {
        irBinaryImplNode.visitChildren(this, scope);
    }

    @Override
    public void visitUnaryMath(UnaryMathNode irUnaryMathNode, Scope scope) {
        irUnaryMathNode.visitChildren(this, scope);
    }

    @Override
    public void visitBinaryMath(BinaryMathNode irBinaryMathNode, Scope scope) {
        irBinaryMathNode.visitChildren(this, scope);
    }

    @Override
    public void visitStringConcatenation(StringConcatenationNode irStringConcatenationNode, Scope scope) {
        irStringConcatenationNode.visitChildren(this, scope);
    }

    @Override
    public void visitBoolean(BooleanNode irBooleanNode, Scope scope) {
        irBooleanNode.visitChildren(this, scope);
    }

    @Override
    public void visitComparison(ComparisonNode irComparisonNode, Scope scope) {
        irComparisonNode.visitChildren(this, scope);
    }

    @Override
    public void visitCast(CastNode irCastNode, Scope scope) {
        irCastNode.visitChildren(this, scope);
    }

    @Override
    public void visitInstanceof(InstanceofNode irInstanceofNode, Scope scope) {
        irInstanceofNode.visitChildren(this, scope);
    }

    @Override
    public void visitConditional(ConditionalNode irConditionalNode, Scope scope) {
        irConditionalNode.visitChildren(this, scope);
    }

    @Override
    public void visitElvis(ElvisNode irElvisNode, Scope scope) {
        irElvisNode.visitChildren(this, scope);
    }

    @Override
    public void visitListInitialization(ListInitializationNode irListInitializationNode, Scope scope) {
        irListInitializationNode.visitChildren(this, scope);
    }

    @Override
    public void visitMapInitialization(MapInitializationNode irMapInitializationNode, Scope scope) {
        irMapInitializationNode.visitChildren(this, scope);
    }

    @Override
    public void visitNewArray(NewArrayNode irNewArrayNode, Scope scope) {
        irNewArrayNode.visitChildren(this, scope);
    }

    @Override
    public void visitNewObject(NewObjectNode irNewObjectNode, Scope scope) {
        irNewObjectNode.visitChildren(this, scope);
    }

    @Override
    public void visitConstant(ConstantNode irConstantNode, Scope scope) {
        irConstantNode.visitChildren(this, scope);
    }

    @Override
    public void visitNull(NullNode irNullNode, Scope scope) {
        irNullNode.visitChildren(this, scope);
    }

    @Override
    public void visitDefInterfaceReference(DefInterfaceReferenceNode irDefInterfaceReferenceNode, Scope scope) {
        irDefInterfaceReferenceNode.visitChildren(this, scope);
    }

    @Override
    public void visitTypedInterfaceReference(TypedInterfaceReferenceNode irTypedInterfaceReferenceNode, Scope scope) {
        irTypedInterfaceReferenceNode.visitChildren(this, scope);
    }

    @Override
    public void visitTypeCaptureReference(TypedCaptureReferenceNode irTypedCaptureReferenceNode, Scope scope) {
        irTypedCaptureReferenceNode.visitChildren(this, scope);
    }

    @Override
    public void visitStatic(StaticNode irStaticNode, Scope scope) {
        irStaticNode.visitChildren(this, scope);
    }

    @Override
    public void visitLoadVariable(LoadVariableNode irLoadVariableNode, Scope scope) {
        irLoadVariableNode.visitChildren(this, scope);
    }

    @Override
    public void visitNullSafeSub(NullSafeSubNode irNullSafeSubNode, Scope scope) {
        irNullSafeSubNode.visitChildren(this, scope);
    }

    @Override
    public void visitLoadDotArrayLengthNode(LoadDotArrayLengthNode irLoadDotArrayLengthNode, Scope scope) {
        irLoadDotArrayLengthNode.visitChildren(this, scope);
    }

    @Override
    public void visitLoadDotDef(LoadDotDefNode irLoadDotDefNode, Scope scope) {
        irLoadDotDefNode.visitChildren(this, scope);
    }

    @Override
    public void visitLoadDot(LoadDotNode irLoadDotNode, Scope scope) {
        irLoadDotNode.visitChildren(this, scope);
    }

    @Override
    public void visitLoadDotShortcut(LoadDotShortcutNode irDotSubShortcutNode, Scope scope) {
        irDotSubShortcutNode.visitChildren(this, scope);
    }

    @Override
    public void visitLoadListShortcut(LoadListShortcutNode irLoadListShortcutNode, Scope scope) {
        irLoadListShortcutNode.visitChildren(this, scope);
    }

    @Override
    public void visitLoadMapShortcut(LoadMapShortcutNode irLoadMapShortcutNode, Scope scope) {
        irLoadMapShortcutNode.visitChildren(this, scope);
    }

    @Override
    public void visitLoadFieldMember(LoadFieldMemberNode irLoadFieldMemberNode, Scope scope) {
        irLoadFieldMemberNode.visitChildren(this, scope);
    }

    @Override
    public void visitLoadBraceDef(LoadBraceDefNode irLoadBraceDefNode, Scope scope) {
        irLoadBraceDefNode.visitChildren(this, scope);
    }

    @Override
    public void visitLoadBrace(LoadBraceNode irLoadBraceNode, Scope scope) {
        irLoadBraceNode.visitChildren(this, scope);
    }

    @Override
    public void visitStoreVariable(StoreVariableNode irStoreVariableNode, Scope scope) {
        irStoreVariableNode.visitChildren(this, scope);
    }

    @Override
    public void visitStoreDotDef(StoreDotDefNode irStoreDotDefNode, Scope scope) {
        irStoreDotDefNode.visitChildren(this, scope);
    }

    @Override
    public void visitStoreDot(StoreDotNode irStoreDotNode, Scope scope) {
        irStoreDotNode.visitChildren(this, scope);
    }

    @Override
    public void visitStoreDotShortcut(StoreDotShortcutNode irDotSubShortcutNode, Scope scope) {
        irDotSubShortcutNode.visitChildren(this, scope);
    }

    @Override
    public void visitStoreListShortcut(StoreListShortcutNode irStoreListShortcutNode, Scope scope) {
        irStoreListShortcutNode.visitChildren(this, scope);
    }

    @Override
    public void visitStoreMapShortcut(StoreMapShortcutNode irStoreMapShortcutNode, Scope scope) {
        irStoreMapShortcutNode.visitChildren(this, scope);
    }

    @Override
    public void visitStoreFieldMember(StoreFieldMemberNode irStoreFieldMemberNode, Scope scope) {
        irStoreFieldMemberNode.visitChildren(this, scope);
    }

    @Override
    public void visitStoreBraceDef(StoreBraceDefNode irStoreBraceDefNode, Scope scope) {
        irStoreBraceDefNode.visitChildren(this, scope);
    }

    @Override
    public void visitStoreBrace(StoreBraceNode irStoreBraceNode, Scope scope) {
        irStoreBraceNode.visitChildren(this, scope);
    }

    @Override
    public void visitInvokeCallDef(InvokeCallDefNode irInvokeCallDefNode, Scope scope) {
        irInvokeCallDefNode.visitChildren(this, scope);
    }

    @Override
    public void visitInvokeCall(InvokeCallNode irInvokeCallNode, Scope scope) {
        irInvokeCallNode.visitChildren(this, scope);
    }

    @Override
    public void visitInvokeCallMember(InvokeCallMemberNode irInvokeCallMemberNode, Scope scope) {
        irInvokeCallMemberNode.visitChildren(this, scope);
    }

    @Override
    public void visitFlipArrayIndex(FlipArrayIndexNode irFlipArrayIndexNode, Scope scope) {
        irFlipArrayIndexNode.visitChildren(this, scope);
    }

    @Override
    public void visitFlipCollectionIndex(FlipCollectionIndexNode irFlipCollectionIndexNode, Scope scope) {
        irFlipCollectionIndexNode.visitChildren(this, scope);
    }

    @Override
    public void visitFlipDefIndex(FlipDefIndexNode irFlipDefIndexNode, Scope scope) {
        irFlipDefIndexNode.visitChildren(this, scope);
    }

    @Override
    public void visitDup(DupNode irDupNode, Scope scope) {
        irDupNode.visitChildren(this, scope);
    }
}
