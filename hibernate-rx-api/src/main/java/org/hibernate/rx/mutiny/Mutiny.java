package org.hibernate.rx.mutiny;

import io.smallrye.mutiny.Uni;
import org.hibernate.rx.RxSession;

import java.util.List;
import java.util.Optional;

import static io.smallrye.mutiny.Uni.createFrom;

public class Mutiny {
	public static class Session {
		private RxSession session;

		public Session(RxSession session) {
			this.session = session;
		}

		public <T> Uni<Optional<T>> find(Class<T> entityClass, Object id) {
			return createFrom().completionStage( session.find(entityClass, id) );
		}

		public <T> Uni<List<T>> find(Class<T> entityClass, Object... ids) {
			return createFrom().completionStage( session.find(entityClass, ids) );
		}

		public <T> T getReference(Class<T> entityClass, Object id) {
			return session.getReference(entityClass, id);
		}

		public Uni<Session> persist(Object entity) {
			return createFrom().completionStage( session.persist(entity) ).map( s -> this);
		}

		public Uni<Session> remove(Object entity) {
			return createFrom().completionStage( session.remove(entity) ).map( s -> this);
		}

		public <T> Uni<T> merge(T object) {
			return createFrom().completionStage( session.merge(object) );
		}

		public Uni<Session> refresh(Object entity) {
			return createFrom().completionStage( session.refresh(entity) ).map( s -> this);
		}

		public Uni<Session> flush() {
			return createFrom().completionStage( session.flush() ).map( s -> this);
		}

		public <T> Uni<Optional<T>> fetch(T association) {
			return createFrom().completionStage( session.fetch(association) );
		}
	}
}
