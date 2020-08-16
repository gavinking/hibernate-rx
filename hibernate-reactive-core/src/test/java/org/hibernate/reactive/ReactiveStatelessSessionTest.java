/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import io.vertx.ext.unit.TestContext;
import org.hibernate.cfg.Configuration;
import org.hibernate.reactive.stage.Stage;
import org.junit.Test;

import javax.persistence.*;
import java.util.Objects;


public class ReactiveStatelessSessionTest extends BaseReactiveTest {

	@Override
	protected Configuration constructConfiguration() {
		Configuration configuration = super.constructConfiguration();
		configuration.addAnnotatedClass( GuineaPig.class );
		return configuration;
	}

	@Test
	public void testStatelessSession(TestContext context) {
		GuineaPig pig = new GuineaPig("Aloi");
		Stage.StatelessSession ss = getSessionFactory().createStatelessSession();
		test(
				context,
				ss.insert(pig)
						.thenCompose( v -> ss.createQuery("from GuineaPig where name=:n", GuineaPig.class)
								.setParameter("n", pig.name)
								.getResultList() )
						.thenAccept( list -> {
							context.assertFalse( list.isEmpty() );
							context.assertEquals(1, list.size());
							assertThatPigsAreEqual(context, pig, list.get(0));
						} )
						.thenCompose( v -> ss.get(GuineaPig.class, pig.id) )
						.thenCompose( p -> {
							assertThatPigsAreEqual(context, pig, p);
							p.name = "X";
							return ss.update(p);
						} )
						.thenCompose( v -> ss.refresh(pig) )
						.thenAccept( v -> context.assertEquals(pig.name, "X") )
						.thenCompose( v -> ss.createQuery("update GuineaPig set name='Y'").executeUpdate() )
						.thenCompose( v -> ss.refresh(pig) )
						.thenAccept( v -> context.assertEquals(pig.name, "Y") )
						.thenCompose( v -> ss.delete(pig) )
						.thenCompose( v -> ss.createQuery("from GuineaPig").getResultList() )
						.thenAccept( list -> context.assertTrue( list.isEmpty() ) )
						.thenAccept( v -> ss.close() )
		);
	}

	@Test
	public void testStatelessSessionWithNative(TestContext context) {
		GuineaPig pig = new GuineaPig("Aloi");
		Stage.StatelessSession ss = getSessionFactory().createStatelessSession();
		test(
				context,
				ss.insert(pig)
						.thenCompose( v -> ss.createNativeQuery("select * from Piggy where name=:n", GuineaPig.class)
								.setParameter("n", pig.name)
								.getResultList() )
						.thenAccept( list -> {
							context.assertFalse( list.isEmpty() );
							context.assertEquals(1, list.size());
							assertThatPigsAreEqual(context, pig, list.get(0));
						} )
						.thenCompose( v -> ss.get(GuineaPig.class, pig.id) )
						.thenCompose( p -> {
							assertThatPigsAreEqual(context, pig, p);
							p.name = "X";
							return ss.update(p);
						} )
						.thenCompose( v -> ss.refresh(pig) )
						.thenAccept( v -> context.assertEquals(pig.name, "X") )
						.thenCompose( v -> ss.createNativeQuery("update Piggy set name='Y'").executeUpdate() )
						.thenCompose( v -> ss.refresh(pig) )
						.thenAccept( v -> context.assertEquals(pig.name, "Y") )
						.thenCompose( v -> ss.delete(pig) )
						.thenCompose( v -> ss.createNativeQuery("select id from Piggy").getResultList() )
						.thenAccept( list -> context.assertTrue( list.isEmpty() ) )
						.thenAccept( v -> ss.close() )
		);
	}

	private void assertThatPigsAreEqual(TestContext context, GuineaPig expected, GuineaPig actual) {
		context.assertNotNull( actual );
		context.assertEquals( expected.getId(), actual.getId() );
		context.assertEquals( expected.getName(), actual.getName() );
	}

	@Entity(name="GuineaPig")
	@Table(name="Piggy")
	public static class GuineaPig {
		@Id @GeneratedValue
		private Integer id;
		private String name;
		@Version
		private int version;

		public GuineaPig() {
		}

		public GuineaPig(String name) {
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
}
