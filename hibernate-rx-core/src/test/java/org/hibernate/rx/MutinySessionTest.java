package org.hibernate.rx;

import io.smallrye.mutiny.Uni;
import io.vertx.axle.sqlclient.Tuple;
import io.vertx.ext.unit.TestContext;
import org.hibernate.cfg.Configuration;
import org.hibernate.rx.mutiny.Mutiny;
import org.junit.Test;

import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletionStage;

public class MutinySessionTest extends BaseRxTest {

	@Override
	protected Configuration constructConfiguration() {
		Configuration configuration = super.constructConfiguration();
		configuration.addAnnotatedClass( GuineaPig.class );
		return configuration;
	}

	private Uni<Integer> populateDB() {
		return Uni.createFrom().completionStage( connection().update( "INSERT INTO Pig (id, name) VALUES (5, 'Aloi')" ) );
	}

	private CompletionStage<Integer> cleanDB() {
		return connection().update( "DELETE FROM Pig" );
	}

	public void after(TestContext context) {
		cleanDB()
				.whenComplete( (res, err) -> {
					// in case cleanDB() fails we
					// stll have to close the factory
					try {
						super.after(context);
					}
					finally {
						context.assertNull( err );
					}
				} )
				.whenComplete( (res, err) -> {
					// in case cleanDB() worked but
					// SessionFactory didn't close
					context.assertNull( err );
				} );
	}

	private Uni<String> selectNameFromId(Integer id) {
		CompletionStage<String> result = connection().preparedQuery(
				"SELECT name FROM Pig WHERE id = $1", Tuple.of(id)).thenApply(
				rowSet -> {
					if (rowSet.size() == 1) {
						// Only one result
						return rowSet.iterator().next().getString(0);
					} else if (rowSet.size() > 1) {
						throw new AssertionError("More than one result returned: " + rowSet.size());
					} else {
						// Size 0
						return null;
					}
				});
		return Uni.createFrom().completionStage(result);
	}

	@Test
	public void reactiveFind1(TestContext context) {
		final GuineaPig expectedPig = new GuineaPig( 5, "Aloi" );
		test(
				context,
				populateDB()
						.onItem().produceUni( i -> openMutinySession() )
						.onItem().produceUni( session -> session.find( GuineaPig.class, expectedPig.getId() ) )
						.onItem().invoke( actualPig -> assertThatPigsAreEqual( context, expectedPig, actualPig ) )
						.convert().toCompletionStage()
		);
	}

	@Test
	public void reactiveFind2(TestContext context) {
		final GuineaPig expectedPig = new GuineaPig( 5, "Aloi" );
		test(
				context,
				populateDB()
						.flatMap( i -> openMutinySession() )
						.flatMap( session -> session.find( GuineaPig.class, expectedPig.getId() ) )
						.onItem().invoke( actualPig -> assertThatPigsAreEqual( context, expectedPig, actualPig ) )
						.convert().toCompletionStage()
		);
	}

	@Test
	public void reactivePersist1(TestContext context) {
		test(
				context,
				openMutinySession()
						.onItem().produceUni( s -> s.persist( new GuineaPig( 10, "Tulip" ) ) )
						.onItem().produceUni( s -> s.flush() )
						.onItem().produceUni( v -> selectNameFromId( 10 ) )
						.onItem().invoke( selectRes -> context.assertEquals( "Tulip", selectRes ) )
						.convert().toCompletionStage()
		);
	}

	@Test
	public void reactivePersist2(TestContext context) {
		test(
				context,
				openMutinySession()
						.flatMap( s -> s.persist( new GuineaPig( 10, "Tulip" ) ) )
						.flatMap( s -> s.flush() )
						.flatMap( v -> selectNameFromId( 10 ) )
						.map( selectRes -> context.assertEquals( "Tulip", selectRes ) )
						.convert().toCompletionStage()
		);
	}

	@Test
	public void reactiveRemoveTransientEntity1(TestContext context) {
		test(
				context,
				populateDB()
						.onItem().produceUni( v -> selectNameFromId( 5 ) )
						.onItem().invoke( name -> context.assertNotNull( name ) )
						.onItem().produceUni( v -> openMutinySession() )
						.onItem().produceUni( session -> session.remove( new GuineaPig( 5, "Aloi" ) ) )
						.onItem().produceUni( session -> session.flush() )
						.onItem().produceUni( v -> selectNameFromId( 5 ) )
						.onItem().invoke( ret -> context.assertNull( ret ) )
						.convert().toCompletionStage()
		);
	}

	@Test
	public void reactiveRemoveTransientEntity2(TestContext context) {
		test(
				context,
				populateDB()
						.flatMap( v -> selectNameFromId( 5 ) )
						.map( name -> context.assertNotNull( name ) )
						.flatMap( v -> openMutinySession() )
						.flatMap( session -> session.remove( new GuineaPig( 5, "Aloi" ) ) )
						.flatMap( session -> session.flush() )
						.flatMap( v -> selectNameFromId( 5 ) )
						.map( ret -> context.assertNull( ret ) )
						.convert().toCompletionStage()
		);
	}

	@Test
	public void reactiveRemoveManagedEntity1(TestContext context) {
		test(
				context,
				populateDB()
						.onItem().produceUni( v -> openMutinySession() )
						.onItem().produceUni( session ->
								session.find( GuineaPig.class, 5 )
										.onItem().produceUni( aloi -> session.remove( aloi.get() ) )
										.onItem().produceUni( v -> session.flush() )
										.onItem().produceUni( v -> selectNameFromId( 5 ) )
										.onItem().invoke( ret -> context.assertNull( ret ) ) )
						.convert().toCompletionStage()
		);
	}

	@Test
	public void reactiveRemoveManagedEntity2(TestContext context) {
		test(
				context,
				populateDB()
						.flatMap( v -> openMutinySession() )
						.flatMap( session ->
							session.find( GuineaPig.class, 5 )
								.flatMap( aloi -> session.remove( aloi.get() ) )
								.flatMap( v -> session.flush() )
								.flatMap( v -> selectNameFromId( 5 ) )
								.map( ret -> context.assertNull( ret ) ) )
								.convert().toCompletionStage()
		);
	}

	@Test
	public void reactiveUpdate(TestContext context) {
		final String NEW_NAME = "Tina";
		test(
				context,
				populateDB()
						.flatMap( v -> openMutinySession() )
						.flatMap( session ->
							session.find( GuineaPig.class, 5 )
								.onItem().invoke( o -> {
									GuineaPig pig = o.orElseThrow( () -> new AssertionError( "Guinea pig not found" ) );
									// Checking we are actually changing the name
									context.assertNotEquals( pig.getName(), NEW_NAME );
									pig.setName( NEW_NAME );
								} )
								.flatMap( v -> session.flush() )
								.flatMap( v -> selectNameFromId( 5 ) )
								.map( name -> context.assertEquals( NEW_NAME, name ) ) )
						.convert().toCompletionStage()
		);
	}

	private void assertThatPigsAreEqual(TestContext context, GuineaPig expected, Optional<GuineaPig> actual) {
		context.assertTrue( actual.isPresent() );
		context.assertEquals( expected.getId(), actual.get().getId() );
		context.assertEquals( expected.getName(), actual.get().getName() );
	}

	@Entity
	@Table(name="Pig")
	public static class GuineaPig {
		@Id
		private Integer id;
		private String name;

		public GuineaPig() {
		}

		public GuineaPig(Integer id, String name) {
			this.id = id;
			this.name = name;
		}

		public Integer getId() {
			return id;
		}

		public void setId(Integer id) {
			this.id = id;
		}

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

		@Override
		public String toString() {
			return id + ": " + name;
		}

		@Override
		public boolean equals(Object o) {
			if ( this == o ) {
				return true;
			}
			if ( o == null || getClass() != o.getClass() ) {
				return false;
			}
			GuineaPig guineaPig = (GuineaPig) o;
			return Objects.equals( name, guineaPig.name );
		}

		@Override
		public int hashCode() {
			return Objects.hash( name );
		}
	}

	protected Uni<Mutiny.Session> openMutinySession() {
		return Uni.createFrom().completionStage( super.openSession() ).map( Mutiny.Session::new );
	}
}
