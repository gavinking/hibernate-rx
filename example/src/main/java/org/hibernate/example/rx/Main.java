package org.hibernate.example.rx;

import org.hibernate.rx.RxSession;
import org.hibernate.rx.RxSessionFactory;

import static javax.persistence.Persistence.createEntityManagerFactory;

public class Main {

	public static void main(String[] args) {

		// obtain a factory for reactive sessions based on the
		// standard JPA configuration properties specified in
		// resources/META-INF/persistence.xml
		RxSessionFactory sessionFactory =
				createEntityManagerFactory("example")
						.unwrap(RxSessionFactory.class);

		Book book = new Book("1-85723-235-6", "Feersum Endjinn", "Iain M. Banks");

		//obtain a reactive session
		RxSession session1 = sessionFactory.openRxSession();
		//persist a Book
		session1.persist(book)
				.thenCompose( $ -> session1.flush() )
				.thenAccept( $ -> session1.close() )
				.toCompletableFuture()
				.join();

		RxSession session2 = sessionFactory.openRxSession();
		//retrieve the Book and print its title
		session2.find(Book.class, book.id)
				.thenAccept( bOOk -> System.out.println(bOOk.title + " is a great book!") )
				.thenAccept( $ -> session2.close() )
				.toCompletableFuture()
				.join();

		RxSession session3 = sessionFactory.openRxSession();
		//retrieve the Book and delete it
		session3.find(Book.class, book.id)
				.thenCompose( bOOk -> session3.remove(bOOk))
				.thenCompose( $ -> session3.flush() )
				.thenAccept( $ -> session2.close() )
				.toCompletableFuture()
				.join();

		System.exit(0);
	}

}
