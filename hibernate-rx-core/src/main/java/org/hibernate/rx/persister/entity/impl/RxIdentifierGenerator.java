package org.hibernate.rx.persister.entity.impl;

import org.hibernate.engine.spi.SharedSessionContractImplementor;

import java.util.concurrent.CompletionStage;

/**
 * A replacement for {@link org.hibernate.id.IdentifierGenerator},
 * which supports a non-blocking method for obtaining the generated
 * identifier.
 *
 * @see TableRxIdentifierGenerator
 * @see SequenceRxIdentifierGenerator
 */
@FunctionalInterface
public interface RxIdentifierGenerator<Id> {
	/**
	 * Returns a generated identifier, via a {@link CompletionStage}.
	 */
	CompletionStage<Id> generate(SharedSessionContractImplementor session);
}
