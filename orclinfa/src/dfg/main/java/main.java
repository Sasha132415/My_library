package dfg.main.java;

import java.sql.*;

public class main {

    private static final String PROTOCOL = "jdbc:postgresql://";        // URL-prefix
    private static final String DRIVER = "org.postgresql.Driver";       // Driver name
    private static final String URL_LOCALE_NAME = "localhost/";         // ваш компьютер + порт по умолчанию

    private static final String DATABASE_NAME = "rut_head_hunter";          // FIXME имя базы

    public static final String DATABASE_URL = PROTOCOL + URL_LOCALE_NAME + DATABASE_NAME;
    public static final String USER_NAME = "postgres";                  // FIXME имя пользователя
    public static final String DATABASE_PASS = "postgres";              // FIXME пароль базы данных

    public static void main(String[] args) {

        // проверка возможности подключения
        checkDriver();
        checkDB();
        System.out.println("Подключение к базе данных | " + DATABASE_URL + "\n");

        // попытка открыть соединение с базой данных, которое java-закроет перед выходом из try-with-resources
        try (Connection connection = DriverManager.getConnection(DATABASE_URL, USER_NAME, DATABASE_PASS)) {
            //TODO show all tables
            getVillains(connection); System.out.println();
            getMinions(connection); System.out.println();
            getContracts(connection); System.out.println();

            // TODO show with param
            getVillainNamed(connection, "Грю", false); System.out.println();// возьмем всех и найдем перебором
            getVillainNamed(connection, "Грю", true); System.out.println(); // тоже самое сделает БД
            getVillainMinions(connection, "Грю"); System.out.println();

            // TODO correction
            addMinion(connection, "Карл", 10); System.out.println();
            correctMinion(connection, "Карл", 4); System.out.println();
            removeMinion(connection, "Карл"); System.out.println();

        } catch (SQLException e) {
            // При открытии соединения, выполнении запросов могут возникать различные ошибки
            // Согласно стандарту SQL:2008 в ситуациях нарушения ограничений уникальности (в т.ч. дублирования данных) возникают ошибки соответствующие статусу (или дочерние ему): SQLState 23000 - Integrity Constraint Violation
            if (e.getSQLState().startsWith("23")){
                System.out.println("Произошло дублирование данных");
            } else throw new RuntimeException(e);
        }
    }

    // region // Проверка окружения и доступа к базе данных

    public static void checkDriver () {
        try {
            Class.forName(DRIVER);
        } catch (ClassNotFoundException e) {
            System.out.println("Нет JDBC-драйвера! Подключите JDBC-драйвер к проекту согласно инструкции.");
            throw new RuntimeException(e);
        }
    }

    public static void checkDB () {
        try {
            Connection connection = DriverManager.getConnection(DATABASE_URL, USER_NAME, DATABASE_PASS);
        } catch (SQLException e) {
            System.out.println("Нет базы данных! Проверьте имя базы, путь к базе или разверните локально резервную копию согласно инструкции");
            throw new RuntimeException(e);
        }
    }

    // endregion

    // region // SELECT-запросы без параметров в одной таблице

    private static void getVillains(Connection connection) throws SQLException{
        // имена столбцов
        String columnName0 = "id", columnName1 = "name", columnName2 = "nickname";
        // значения ячеек
        int param0 = -1;
        String param1 = null, param2 = null;

        Statement statement = connection.createStatement();     // создаем оператор для простого запроса (без параметров)
        ResultSet rs = statement.executeQuery("SELECT * FROM villain;"); // выполняем запроса на поиск и получаем список ответов

        while (rs.next()) {  // пока есть данные, продвигаться по ним
            param2 = rs.getString(columnName2); // значение ячейки, можно получить по имени; по умолчанию возвращается строка
            param1 = rs.getString(columnName1);
            param0 = rs.getInt(columnName0);    // если точно уверены в типе данных ячейки, можно его сразу преобразовать
            System.out.println(param0 + " | " + param1 + " | " + param2);
        }
    }

    static void getMinions (Connection connection) throws SQLException {
        // значения ячеек
        int param0 = -1, param2 = -1;
        String param1 = null;

        Statement statement = connection.createStatement();                 // создаем оператор для простого запроса (без параметров)
        ResultSet rs = statement.executeQuery("SELECT * FROM minion;");  // выполняем запроса на поиск и получаем список ответов

        while (rs.next()) {  // пока есть данные
            param0 = rs.getInt(1); // значение ячейки, можно также получить по порядковому номеру (начиная с 1)
            param1 = rs.getString(2);
            param2 = rs.getInt(3);
            System.out.println(param0 + " | " + param1 + " | " + param2);
        }
    }

    static void getContracts (Connection connection) throws SQLException {
        String param = "";

        Statement statement = connection.createStatement();             // создаем оператор для простого запроса (без параметров)
        ResultSet rs = statement.executeQuery("SELECT * FROM contract;");   // выполняем запроса на поиск и получаем список ответов

        int count = rs.getMetaData().getColumnCount();  // сколько столбцов в ответе
        for (int i = 1; i <= count; i++){
            // что в этом столбце?
            System.out.println("position - " + i +
                    ", label - " + rs.getMetaData().getColumnLabel(i) +
                    ", type - " + rs.getMetaData().getColumnType(i) +
                    ", typeName - " + rs.getMetaData().getColumnTypeName(i) +
                    ", javaClass - " + rs.getMetaData().getColumnClassName(i)
            );
        }
        System.out.println();

        while (rs.next()) {  // пока есть данные
            for (int i = 1; i <= count; i++) {
                param += rs.getString(i);
                if (i != count) param += " | ";
            }
            System.out.println(param);
            param = "";
        }
    }

    // endregion

    // region // SELECT-запросы с параметрами и объединением таблиц

    private static void getVillainNamed(Connection connection, String name, boolean fromSQL) throws SQLException {
        if (name == null || name.isBlank()) return;// проверка "на дурака"

        if (fromSQL) {
            getVillainNamed(connection, name);               // если флаг верен, то выполняем аналогичный запрос c условием (WHERE)
        } else {
            long time = System.currentTimeMillis();
            Statement statement = connection.createStatement();      // создаем оператор для простого запроса (без параметров)
            ResultSet rs = statement.executeQuery(
                    "SELECT id, name, nickname " +
                            "FROM villain");
            while (rs.next()) {  // пока есть данные перебираем их
                if (rs.getString(2).contains(name)) { // и выводим только определенный параметр
                    System.out.println(rs.getInt(1) + " | " + rs.getString(2) + " | " + rs.getString(3));
                }
            }
            System.out.println("SELECT ALL and FIND (" + (System.currentTimeMillis() - time) + " мс.)");
        }
    }

    private static void getVillainNamed(Connection connection, String name) throws SQLException {
        if (name == null || name.isBlank()) return; // проверка "на дурака"
        name = '%' + name + '%'; // переданное значение может быть дополнено сначала и в конце (часть слова)

        long time = System.currentTimeMillis();
        PreparedStatement statement = connection.prepareStatement(
                "SELECT id, name, nickname " +
                        "FROM villain " +
                        "WHERE name LIKE ?;");  // создаем оператор шаблонного-запроса с "включаемыми" параметрами - ?
        statement.setString(1, name);           // "безопасное" добавление параметров в запрос; с учетом их типа и порядка (индексация с 1)
        ResultSet rs = statement.executeQuery();// выполняем запроса на поиск и получаем список ответов

        while (rs.next()) {  // пока есть данные перебираем их и выводим
            System.out.println(rs.getInt(1) + " | " + rs.getString(2) + " | " + rs.getString(3));
        }
        System.out.println("SELECT with WHERE (" + (System.currentTimeMillis() - time) + " мс.)");
    }

    private static void getVillainMinions(Connection connection, String name) throws SQLException {
        if (name == null || name.isBlank()) return;// проверка "на дурака"
        name = '%' + name + '%'; // переданное значение может быть дополнено сначала и в конце (часть слова)

        long time = System.currentTimeMillis();
        PreparedStatement statement = connection.prepareStatement(
                "SELECT villain.name, villain.nickname, minion.name, contract.payment " +
                        "FROM villain " +
                        "JOIN contract ON villain.id = contract.id_villain " +
                        "JOIN minion ON minion.id = contract.id_minion " +
                        "WHERE villain.name LIKE ?;");       // создаем оператор шаблонного-запроса с "включаемыми" параметрами - ?
        statement.setString(1, name);      // "безопасное" добавление параметров в запрос; с учетом их типа и порядка (индексация с 1)
        ResultSet rs = statement.executeQuery();    // выполняем запроса на поиск и получаем список ответов

        while (rs.next()) {  // пока есть данные перебираем их и выводим
            System.out.println(rs.getString(1) + " | " + rs.getString(2) + " | " + rs.getString(3) + " | " + rs.getString(4));
        }
        System.out.println("SELECT with WHERE (" + (System.currentTimeMillis() - time) + " мс.)");
    }

    // endregion

    // region // CUD-запросы на добавление, изменение и удаление записей

    private static void addMinion (Connection connection, String name, int eyesCount)  throws SQLException {
        if (name == null || name.isBlank() || eyesCount < 0) return;

        PreparedStatement statement = connection.prepareStatement(
                "INSERT INTO minion(name, eyes_count) VALUES (?, ?) returning id;", Statement.RETURN_GENERATED_KEYS);    // создаем оператор шаблонного-запроса с "включаемыми" параметрами - ?
        statement.setString(1, name);    // "безопасное" добавление имени
        statement.setInt(2, eyesCount);  // "безопасное" добавление количества глаз

        int count = statement.executeUpdate();  // выполняем запрос на коррекцию и возвращаем количество измененных строк

        ResultSet rs = statement.getGeneratedKeys(); // прочитать запрошенные данные от БД
        if (rs.next()) { // прокрутить к первой записи, если они есть
            System.out.println("Идентификатор миньона " + rs.getInt(1));
        }

        System.out.println("INSERTed " + count + " minion");
        getMinions(connection);
    }

    private static void correctMinion (Connection connection, String name, int eyesCount) throws SQLException {
        if (name == null || name.isBlank() || eyesCount < 0) return;

        PreparedStatement statement = connection.prepareStatement("UPDATE minion SET eyes_count=? WHERE name=?;");
        statement.setInt(1, eyesCount); // сначала что передаем
        statement.setString(2, name);   // затем по чему ищем

        int count = statement.executeUpdate();  // выполняем запрос на коррекцию и возвращаем количество измененных строк

        System.out.println("UPDATEd " + count + " minions");
        getMinions(connection);
    }

    private static void removeMinion(Connection connection, String name) throws SQLException {
        if (name == null || name.isBlank()) return;

        PreparedStatement statement = connection.prepareStatement("DELETE from minion WHERE name=?;");
        statement.setString(1, name);

        int count = statement.executeUpdate(); // выполняем запрос на удаление и возвращаем количество измененных строк
        System.out.println("DELETEd " + count + " minions");
        getMinions(connection);
    }

    // endregion
}
