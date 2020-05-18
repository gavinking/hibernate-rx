package org.hibernate.rx.event.impl;

import org.hibernate.CacheMode;
import org.hibernate.HibernateException;
import org.hibernate.LockMode;
import org.hibernate.TransientObjectException;
import org.hibernate.action.internal.OrphanRemovalAction;
import org.hibernate.bytecode.enhance.spi.LazyPropertyInitializer;
import org.hibernate.engine.internal.CascadePoint;
import org.hibernate.engine.internal.ForeignKeys;
import org.hibernate.engine.internal.Nullability;
import org.hibernate.engine.spi.EntityEntry;
import org.hibernate.engine.spi.EntityKey;
import org.hibernate.engine.spi.PersistenceContext;
import org.hibernate.engine.spi.SessionImplementor;
import org.hibernate.engine.spi.Status;
import org.hibernate.event.internal.OnUpdateVisitor;
import org.hibernate.event.service.spi.JpaBootstrapSensitive;
import org.hibernate.event.spi.DeleteEvent;
import org.hibernate.event.spi.DeleteEventListener;
import org.hibernate.event.spi.EventSource;
import org.hibernate.internal.CoreLogging;
import org.hibernate.internal.CoreMessageLogger;
import org.hibernate.internal.util.collections.IdentitySet;
import org.hibernate.jpa.event.spi.CallbackRegistry;
import org.hibernate.jpa.event.spi.CallbackRegistryConsumer;
import org.hibernate.persister.entity.EntityPersister;
import org.hibernate.pretty.MessageHelper;
import org.hibernate.rx.RxSessionInternal;
import org.hibernate.rx.engine.impl.Cascade;
import org.hibernate.rx.engine.impl.CascadingActions;
import org.hibernate.rx.engine.impl.RxEntityDeleteAction;
import org.hibernate.rx.engine.spi.RxActionQueue;
import org.hibernate.rx.event.spi.RxDeleteEventListener;
import org.hibernate.rx.persister.entity.impl.RxEntityPersister;
import org.hibernate.rx.util.impl.RxUtil;
import org.hibernate.type.Type;
import org.hibernate.type.TypeHelper;

import java.io.Serializable;
import java.util.Set;
import java.util.concurrent.CompletionStage;

import static org.hibernate.rx.engine.impl.Cascade.fetchLazyAssociationsBeforeCascade;

/**
 * A reactific {@link org.hibernate.event.internal.DefaultDeleteEventListener}.
 */
public class DefaultRxDeleteEventListener
		implements DeleteEventListener, RxDeleteEventListener, CallbackRegistryConsumer, JpaBootstrapSensitive {
	private static final CoreMessageLogger LOG = CoreLogging.messageLogger( DefaultRxDeleteEventListener.class );

	private CallbackRegistry callbackRegistry;
	private boolean jpaBootstrap;

	@Override
	public void injectCallbackRegistry(CallbackRegistry callbackRegistry) {
		this.callbackRegistry = callbackRegistry;
	}

	@Override
	public void wasJpaBootstrap(boolean wasJpaBootstrap) {
		this.jpaBootstrap = wasJpaBootstrap;
	}

	/**
	 * Handle the given delete event.
	 *
	 * @param event The delete event to be handled.
	 *
	 */
	public void onDelete(DeleteEvent event) {
		throw new UnsupportedOperationException();
	}

	/**
	 * Handle the given delete event.  This is the cascaded form.
	 *
	 * @param event The delete event.
	 * @param transientEntities The cache of entities already deleted
	 *
	 */
	public void onDelete(DeleteEvent event, Set transientEntities) {
		throw new UnsupportedOperationException();
	}

	/**
	 * Handle the given delete event.
	 *
	 * @param event The delete event to be handled.
	 *
	 */
	public CompletionStage<Void> rxOnDelete(DeleteEvent event) throws HibernateException {
		return rxOnDelete( event, new IdentitySet() );
	}

	/**
	 * Handle the given delete event.  This is the cascaded form.
	 *
	 * @param event The delete event.
	 * @param transientEntities The cache of entities already deleted
	 *
	 */
	public CompletionStage<Void> rxOnDelete(DeleteEvent event, IdentitySet transientEntities) throws HibernateException {

		final EventSource source = event.getSession();

		final PersistenceContext persistenceContext = source.getPersistenceContextInternal();
		Object entity = persistenceContext.unproxyAndReassociate( event.getObject() );

		EntityEntry entityEntry = persistenceContext.getEntry( entity );

		if ( entityEntry == null ) {
			LOG.trace( "Entity was not persistent in delete processing" );

			final EntityPersister persister = source.getEntityPersister( event.getEntityName(), entity );

			return isTransient( persister.getEntityName(), entity, source.getSession() )
					.thenCompose( trans -> {
						if ( trans ) {
							// EARLY EXIT!!!
							return deleteTransientEntity( source, entity, event.isCascadeDeleteEnabled(), persister, transientEntities );
						}
						performDetachedEntityDeletionCheck( event );

						final Serializable id = persister.getIdentifier( entity, source );

						if ( id == null ) {
							throw new TransientObjectException(
									"the detached instance passed to delete() had a null identifier"
							);
						}

						final EntityKey key = source.generateEntityKey( id, persister );

						persistenceContext.checkUniqueness( key, entity );

						new OnUpdateVisitor( source, id, entity ).process( entity, persister );

						final Object version = persister.getVersion( entity );

						EntityEntry entry = persistenceContext.addEntity(
								entity,
								( persister.isMutable() ? Status.MANAGED : Status.READ_ONLY ),
								persister.getPropertyValues( entity ),
								key,
								version,
								LockMode.NONE,
								true,
								persister,
								false
						);
						persister.afterReassociate( entity, source );

						callbackRegistry.preRemove( entity );

						return deleteEntity(
								source,
								entity,
								entry,
								event.isCascadeDeleteEnabled(),
								event.isOrphanRemovalBeforeUpdates(),
								persister,
								transientEntities
						)
						.thenAccept( v -> {
							if ( source.getFactory().getSessionFactoryOptions().isIdentifierRollbackEnabled() ) {
								persister.resetIdentifier( entity, id, version, source );
							}
						} );
					} );
		}
		else {
			LOG.trace( "Deleting a persistent instance" );

			if ( entityEntry.getStatus() == Status.DELETED || entityEntry.getStatus() == Status.GONE ) {
				LOG.trace( "Object was already deleted" );
				return RxUtil.nullFuture();
			}
			final EntityPersister persister = entityEntry.getPersister();
			Serializable id = entityEntry.getId();
			Object version = entityEntry.getVersion();

			callbackRegistry.preRemove( entity );

			return deleteEntity(
					source,
					entity,
					entityEntry,
					event.isCascadeDeleteEnabled(),
					event.isOrphanRemovalBeforeUpdates(),
					persister,
					transientEntities
			)
			.thenAccept( v -> {
				if ( source.getFactory().getSessionFactoryOptions().isIdentifierRollbackEnabled() ) {
					persister.resetIdentifier( entity, id, version, source );
				}
			} );
		}
	}

	/**
	 * Called when we have recognized an attempt to delete a detached entity.
	 * <p>
	 * This is perfectly valid in Hibernate usage; JPA, however, forbids this.
	 * Thus, this is a hook for HEM to affect this behavior.
	 *
	 * @param event The event.
	 */
	protected void performDetachedEntityDeletionCheck(DeleteEvent event) {
		if ( jpaBootstrap ) {
			disallowDeletionOfDetached( event );
		}
		// ok in normal Hibernate usage to delete a detached entity; JPA however
		// forbids it, thus this is a hook for HEM to affect this behavior
	}

	private void disallowDeletionOfDetached(DeleteEvent event) {
		EventSource source = event.getSession();
		String entityName = event.getEntityName();
		EntityPersister persister = source.getEntityPersister( entityName, event.getObject() );
		Serializable id = persister.getIdentifier( event.getObject(), source );
		entityName = entityName == null ? source.guessEntityName( event.getObject() ) : entityName;
		throw new IllegalArgumentException( "Removing a detached instance " + entityName + "#" + id );
	}

	/**
	 * We encountered a delete request on a transient instance.
	 * <p>
	 * This is a deviation from historical Hibernate (pre-3.2) behavior to
	 * align with the JPA spec, which states that transient entities can be
	 * passed to remove operation in which case cascades still need to be
	 * performed.
	 *
	 * @param session The session which is the source of the event
	 * @param entity The entity being delete processed
	 * @param cascadeDeleteEnabled Is cascading of deletes enabled
	 * @param persister The entity persister
	 * @param transientEntities A cache of already visited transient entities
	 * (to avoid infinite recursion).
	 */
	protected CompletionStage<Void> deleteTransientEntity(
			EventSource session,
			Object entity,
			boolean cascadeDeleteEnabled,
			EntityPersister persister,
			IdentitySet transientEntities) {
		LOG.handlingTransientEntity();
		if ( transientEntities.contains( entity ) ) {
			LOG.trace( "Already handled transient entity; skipping" );
			return RxUtil.nullFuture();
		}
		transientEntities.add( entity );
		return cascadeBeforeDelete( session, persister, entity, null, transientEntities )
				.thenCompose( v -> cascadeAfterDelete( session, persister, entity, transientEntities ) );
	}

	/**
	 * Perform the entity deletion.  Well, as with most operations, does not
	 * really perform it; just schedules an action/execution with the
	 * {@link org.hibernate.engine.spi.ActionQueue} for execution during flush.
	 *
	 * @param session The originating session
	 * @param entity The entity to delete
	 * @param entityEntry The entity's entry in the {@link PersistenceContext}
	 * @param isCascadeDeleteEnabled Is delete cascading enabled?
	 * @param persister The entity persister.
	 * @param transientEntities A cache of already deleted entities.
	 */
	protected CompletionStage<?> deleteEntity(
			final EventSource session,
			final Object entity,
			final EntityEntry entityEntry,
			final boolean isCascadeDeleteEnabled,
			final boolean isOrphanRemovalBeforeUpdates,
			final EntityPersister persister,
			final IdentitySet transientEntities) {

		if ( LOG.isTraceEnabled() ) {
			LOG.tracev(
					"Deleting {0}",
					MessageHelper.infoString( persister, entityEntry.getId(), session.getFactory() )
			);
		}

		final PersistenceContext persistenceContext = session.getPersistenceContextInternal();
		final Type[] propTypes = persister.getPropertyTypes();
		final Object version = entityEntry.getVersion();

		final Object[] currentState;
		if ( entityEntry.getLoadedState() == null ) {
			//ie. the entity came in from update()
			currentState = persister.getPropertyValues( entity );
		}
		else {
			currentState = entityEntry.getLoadedState();
		}

		final Object[] deletedState = createDeletedState( persister, currentState, session );
		entityEntry.setDeletedState( deletedState );

		session.getInterceptor().onDelete(
				entity,
				entityEntry.getId(),
				deletedState,
				persister.getPropertyNames(),
				propTypes
		);

		// before any callbacks, etc, so subdeletions see that this deletion happened first
		persistenceContext.setEntryStatus( entityEntry, Status.DELETED );
		final EntityKey key = session.generateEntityKey( entityEntry.getId(), persister );

		return cascadeBeforeDelete( session, persister, entity, entityEntry, transientEntities )
				.thenAccept( v -> {

					new ForeignKeys.Nullifier(
							entity,
							true,
							false,
							session,
							persister
					).nullifyTransientReferences( entityEntry.getDeletedState() );
					new Nullability( session ).checkNullability(
							entityEntry.getDeletedState(),
							persister,
							Nullability.NullabilityCheckType.DELETE
					);
					persistenceContext.registerNullifiableEntityKey( key );

					RxActionQueue actionQueue = actionQueue( session );

					if ( isOrphanRemovalBeforeUpdates ) {
						// TODO: The removeOrphan concept is a temporary "hack" for HHH-6484.  This should be removed once action/task
						// ordering is improved.
						actionQueue.addAction(
								new OrphanRemovalAction(
										entityEntry.getId(),
										deletedState,
										version,
										entity,
										persister,
										isCascadeDeleteEnabled,
										session
								)
						);
					}
					else {
						// Ensures that containing deletions happen before sub-deletions
						actionQueue.addAction(
								new RxEntityDeleteAction(
										entityEntry.getId(),
										deletedState,
										version,
										entity,
										persister,
										isCascadeDeleteEnabled,
										session
								)
						);
					}
				} )
				.thenCompose( v -> cascadeAfterDelete( session, persister, entity, transientEntities ) );

	}

	private RxActionQueue actionQueue(EventSource session) {
		return session.unwrap( RxSessionInternal.class ).getRxActionQueue();
	}

	private Object[] createDeletedState(EntityPersister persister, Object[] currentState, EventSource session) {
		Type[] propTypes = persister.getPropertyTypes();
		final Object[] deletedState = new Object[propTypes.length];
//      TypeFactory.deepCopy( currentState, propTypes, persister.getPropertyUpdateability(), deletedState, session );
		boolean[] copyability = new boolean[propTypes.length];
		java.util.Arrays.fill( copyability, true );
		TypeHelper.deepCopy( currentState, propTypes, copyability, deletedState, session );
		return deletedState;
	}

	protected CompletionStage<?> cascadeBeforeDelete(
			EventSource session,
			EntityPersister persister,
			Object entity,
			EntityEntry entityEntry,
			IdentitySet transientEntities) throws HibernateException {
		// cascade-delete to collections BEFORE the collection owner is deleted
		return fetchLazyAssociationsBeforeCascade( CascadingActions.DELETE, persister, entity, session )
				.thenCompose(
						v -> new Cascade<>(
								CascadingActions.DELETE,
								CascadePoint.AFTER_INSERT_BEFORE_DELETE,
								persister, entity, transientEntities, session
						).cascade()
				);
	}

	protected CompletionStage<Void> cascadeAfterDelete(
			EventSource session,
			EntityPersister persister,
			Object entity,
			IdentitySet transientEntities) throws HibernateException {
		// cascade-delete to many-to-one AFTER the parent was deleted
		return new Cascade<>(
				CascadingActions.DELETE,
				CascadePoint.BEFORE_INSERT_AFTER_DELETE,
				persister, entity, transientEntities, session
		).cascade();
	}

	/**
	 * Is this instance, which we know is not persistent, actually transient?
	 * <p/>
	 * If <tt>assumed</tt> is non-null, don't hit the database to make the determination, instead assume that
	 * value; the client code must be prepared to "recover" in the case that this assumed result is incorrect.
	 *
	 * @param entityName The name of the entity
	 * @param entity The entity instance
	 * @param session The session
	 *
	 * @return {@code true} if the given entity is transient (unsaved)
	 */
	public static CompletionStage<Boolean> isTransient(String entityName, Object entity,
									  SessionImplementor session) {
		if ( entity == LazyPropertyInitializer.UNFETCHED_PROPERTY ) {
			// an unfetched association can only point to
			// an entity that already exists in the db
			return RxUtil.completedFuture(false);
		}

		// let the interceptor inspect the instance to decide
		Boolean isUnsaved = session.getInterceptor().isTransient( entity );
		if ( isUnsaved != null ) {
			return RxUtil.completedFuture(isUnsaved);
		}

		// let the persister inspect the instance to decide
		return ( (RxEntityPersister) session.getEntityPersister( entityName, entity ) )
				.rxIsTransient( entity, session );

	}

}
