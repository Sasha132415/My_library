package My_library;

import java.sql.*;

public class main {
    private static final String PROTOCOL = "jdbc:postgresql://";        // URL-prefix
    private static final String DRIVER = "org.postgresql.Driver";       // Driver name
    private static final String URL_LOCALE_NAME = "localhost/";         // ваш компьютер + порт по умолчанию
    private static final String DATABASE_NAME = "My_Library";          // FIXME имя базы
    public static final String DATABASE_URL = PROTOCOL + URL_LOCALE_NAME + DATABASE_NAME;
    public static final String USER_NAME = "postgres";                  // FIXME имя пользователя
    public static final String DATABASE_PASS = "postgres";              // FIXME пароль базы данных

    public static void main(String[] args) {
        checkDriver();
        checkDB();
        System.out.println("Подключение к базе данных | " + DATABASE_URL + "\n");
        try (Connection connection = DriverManager.getConnection(DATABASE_URL, USER_NAME, DATABASE_PASS)) {

            getQuantityBooks(connection);
            System.out.println();
            getStatusLibrary(connection);
            System.out.println();
            getNewUser(connection);
            System.out.println();
            getNewBook(connection);
            System.out.println();
            getUser(connection);
            System.out.println();
            getBook(connection);
            System.out.println();
            getFIO(connection);
            System.out.println();

            addNewUser(connection, 6, "Максим", "Марцинкевич", "Сергеевич", 1, 2);
            System.out.println();
            addNewBook(connection, 7, 3, 12, "Пустая могила", "Джонатан Страуд", 43);
            System.out.println();
            removeUser(connection, "Максим");
            removeBook(connection, "Пустая могила");


        } catch (SQLException e) {
            if (e.getSQLState().startsWith("23")) {
                System.out.println("Произошло дублирование данных");
            } else throw new RuntimeException(e);
        }
    }

    public static void checkDriver() {
        try {
            Class.forName(DRIVER);
        } catch (ClassNotFoundException e) {
            System.out.println("Нет JDBC-драйвера! Подключите JDBC-драйвер к проекту согласно инструкции.");
            throw new RuntimeException(e);
        }
    }

    public static void checkDB() {
        try {
            Connection connection = DriverManager.getConnection(DATABASE_URL, USER_NAME, DATABASE_PASS);
        } catch (SQLException e) {
            System.out.println("Нет базы данных! Проверьте имя базы, путь к базе или разверните локально резервную копию согласно инструкции");
            throw new RuntimeException(e);
        }
    }

    private static void getQuantityBooks(Connection connection) throws SQLException {
        System.out.println("ПЕРВЫЙ ЗАПРОС");
        String columnName4 = "quantity_book";
        int quantityBook;

        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery("SELECT quantity_book FROM books WHERE name_book = 'Юшка';");

        while (rs.next()) {
            quantityBook = rs.getInt(columnName4);
            System.out.println("Количество книг Юшка: " + quantityBook);
        }
    }

    private static void getStatusLibrary(Connection connection) throws SQLException {
        System.out.println("ВТОРОЙ ЗАПРОС");
        String columnName4 = "status_library";
        String statusLibrary;

        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery("SELECT status_library FROM library_lib WHERE id_library = 2;");

        while (rs.next()) {
            statusLibrary = rs.getString(columnName4);
            if (statusLibrary.equals("t")) {
                System.out.println("Статус библиотеки: " + statusLibrary + "rue");
            } else {
                System.out.println("Статус библиотеки: " + statusLibrary + "alse");
            }
        }
    }

    private static void addNewUser(Connection connection, int idUser, String name, String secondName, String patronymic, int course, int id_rank) throws SQLException {
        if (name == null || name.isBlank() || course < 0) return;

        PreparedStatement statement = connection.prepareStatement(
                "INSERT INTO users(id_users, name, second_name, patronymic, course, id_rank) VALUES (?, ?, ?, ?, ?, ?);", Statement.RETURN_GENERATED_KEYS);    // создаем оператор шаблонного-запроса с "включаемыми" параметрами - ?
        statement.setInt(1, idUser);
        statement.setString(2, name);
        statement.setString(3, secondName);
        statement.setString(4, patronymic);
        statement.setInt(5, course);
        statement.setInt(6, id_rank);

        int count = statement.executeUpdate();

        ResultSet rs = statement.getGeneratedKeys();

        if (rs.next()) {
            System.out.println("Идентификатор пользователя " + rs.getInt(1));
        }
        System.out.println("INSERTED " + count + " user");
    }

    private static void getNewUser(Connection connection) throws SQLException {
        System.out.println("ТРЕТИЙ ЗАПРОС");
        Statement statement1 = connection.createStatement();
        ResultSet rs = statement1.executeQuery("SELECT * FROM users WHERE id_users = 6;");

        while (rs.next()) {
            System.out.println((rs.getInt(1) + " " + rs.getString(2) + " " + rs.getString(3) + " " + rs.getString(4)) + " " + rs.getInt(5) + " " + rs.getInt(6));
        }
    }

    private static void addNewBook(Connection connection, int idBook, int idLibraryBook, int numberShelf, String nameBook, String authorBook, int quantityBook) throws SQLException {
        if (nameBook == null || nameBook.isBlank() || quantityBook < 0) return;

        PreparedStatement statement = connection.prepareStatement(
                "INSERT INTO books(id_book, id_library_book, number_shelf, name_book, author_book, quantity_book) VALUES (?, ?, ?, ?, ?, ?);", Statement.RETURN_GENERATED_KEYS);    // создаем оператор шаблонного-запроса с "включаемыми" параметрами - ?
        statement.setInt(1, idBook);
        statement.setInt(2, idLibraryBook);
        statement.setInt(3, numberShelf);
        statement.setString(4, nameBook);
        statement.setString(5, authorBook);
        statement.setInt(6, quantityBook);


        int count = statement.executeUpdate();

        ResultSet rs = statement.getGeneratedKeys();

        if (rs.next()) {
            System.out.println("Идентификатор книги " + rs.getInt(1));
        }
        System.out.println("INSERTed " + count + " book");
    }

    private static void getNewBook(Connection connection) throws SQLException {
        System.out.println("ЧЕТВЁРТЫЙ ЗАПРОС");

        Statement statement1 = connection.createStatement();
        ResultSet rs = statement1.executeQuery("SELECT * FROM books WHERE id_book = 7;");

        while (rs.next()) {
            System.out.println((rs.getInt(1) + " " + rs.getInt(2) + " " + rs.getInt(3) + " " + rs.getString(4)) + " " + rs.getString(5) + " " + rs.getInt(6));
        }
    }

    private static void removeUser(Connection connection, String name) throws SQLException {
        if (name == null || name.isBlank()) return;

        PreparedStatement statement = connection.prepareStatement("DELETE from users WHERE name = ?;");
        statement.setString(1, name);
        getUser(connection);

        int count = statement.executeUpdate();
        System.out.println("ADD " + count + " course");
    }

    private static void getUser(Connection connection) throws SQLException {
        System.out.println("ПЯТЫЙ ЗАПРОС");
        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery("SELECT * FROM users;");


        while (rs.next()) {
            System.out.println((rs.getInt(1) + " " + rs.getString(2) + " " + rs.getString(3) + " " + rs.getString(4)) + " " + rs.getInt(5) + " " + rs.getInt(6));
        }
    }


    private static void removeBook(Connection connection, String name) throws SQLException {
        if (name == null || name.isBlank()) return;

        PreparedStatement statement = connection.prepareStatement("DELETE from books WHERE name_book = ?;");
        statement.setString(1, name);
        getBook(connection);

        int count = statement.executeUpdate();
        System.out.println("DELETED " + count + " book");
    }

    private static void getBook(Connection connection) throws SQLException {
        System.out.println("ШЕСТОЙ ЗАПРОС");
        Statement statement1 = connection.createStatement();
        ResultSet rs = statement1.executeQuery("SELECT * FROM books;");

        while (rs.next()) {
            System.out.println((rs.getInt(1) + " " + rs.getInt(2) + " " + rs.getInt(3) + " " + rs.getString(4)) + " " + rs.getString(5) + " " + rs.getInt(6));
        }
    }

    private static void getFIO(Connection connection) throws SQLException {
        System.out.println("CЕДЬМОЙ ЗАПРОС");
        Statement statement1 = connection.createStatement();
        ResultSet rs = statement1.executeQuery("SELECT name, second_name, patronymic FROM users WHERE name = 'Максим';");

        while (rs.next()) {
            System.out.println(rs.getString(1) + " " + rs.getString(2) + " " + rs.getString(3));
        }
    }
}