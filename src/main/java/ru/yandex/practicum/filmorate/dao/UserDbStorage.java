package ru.yandex.practicum.filmorate.dao;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.simple.SimpleJdbcInsert;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Repository;
import ru.yandex.practicum.filmorate.exceptions.UserNotFoundException;
import ru.yandex.practicum.filmorate.model.Film;
import ru.yandex.practicum.filmorate.model.Genre;
import ru.yandex.practicum.filmorate.model.Mpa;
import ru.yandex.practicum.filmorate.model.User;
import ru.yandex.practicum.filmorate.storage.UserStorage;

import java.time.LocalDate;
import java.util.*;

//класс DAO - объект доступа к данным. Необходимо написать
// соответствующие мапперы и методы, позволяющие сохранять
// пользователей в БД и получать их из неё

@Component("userDbStorage")
@Repository
public class UserDbStorage implements UserStorage {

    public static final String FIND_USER_BY_ID_IN_TABLE_SQL = "SELECT * FROM USERS WHERE USER_ID=?";

    private final JdbcTemplate jdbcTemplate;
    private final FilmDbStorage filmDbStorage;
    private final MpaDbStorage mpaDbStorage;
    @Autowired
    private UserMapper userMapper;

    @Autowired
    public UserDbStorage(JdbcTemplate jdbcTemplate, FilmDbStorage filmDbStorage, MpaDbStorage mpaDbStorage) {
        this.jdbcTemplate = jdbcTemplate;
        this.filmDbStorage = filmDbStorage;
        this.mpaDbStorage = mpaDbStorage;
    }

    @Override
    public User addUser(User user) {
        SimpleJdbcInsert insertIntoUser = new SimpleJdbcInsert(jdbcTemplate)
                .withTableName("USERS")
                .usingGeneratedKeyColumns("USER_ID");
        final Map<String, Object> parameters = new HashMap<>();
        parameters.put("EMAIL", user.getEmail());
        parameters.put("LOGIN", user.getLogin());
        parameters.put("USER_NAME", user.getName());
        parameters.put("BIRTHDAY", user.getBirthday());
        Number newId = insertIntoUser.executeAndReturnKey(parameters);
        user.setId(newId.longValue());
        return user;
    }

    @Override
    public User updateUser(long userId, User user) {
        User userExist = jdbcTemplate.query(FIND_USER_BY_ID_IN_TABLE_SQL
                , new Object[]{userId}, userMapper).stream().findAny().orElse(null);
        if(userExist == null) {
            throw new UserNotFoundException("Такого пользователя нет");
        } else {
            String sqlQuery = "UPDATE USERS SET EMAIL=?, LOGIN=?, USER_NAME=?, BIRTHDAY=? WHERE USER_ID=?";
            jdbcTemplate.update(sqlQuery,
                    user.getEmail(),
                    user.getLogin(),
                    user.getName(),
                    user.getBirthday(),
                    userId);
        }
        return user;
    }

    @Override
    public List<User> findAllUser() {
        String sql = "SELECT * FROM USERS";
        return jdbcTemplate.query(sql, userMapper);
    }

    @Override
    public User findUserById(long userId) {
        User user;
        User userExist = jdbcTemplate.query(FIND_USER_BY_ID_IN_TABLE_SQL
                , new Object[]{userId}, userMapper).stream().findAny().orElse(null);
        if(userExist == null) {
            throw new UserNotFoundException("Такого пользователя нет");
        } else {
            user = jdbcTemplate.query("SELECT * FROM USERS WHERE USER_ID=?", new Object[]{userId}, userMapper)
                    .stream().findAny().orElse(null);
        }
        return user;
    }

    @Override
    public void addFriend(long userId, long friendId) {
        User userExist = jdbcTemplate.query(FIND_USER_BY_ID_IN_TABLE_SQL
                , new Object[]{friendId}, userMapper).stream().findAny().orElse(null);
        if(userExist == null) {
            throw new UserNotFoundException("Такого пользователя нет");
        } else {
            String sqlQuery = "SELECT count(*) FROM FRIENDSHIP where USER1_ID=? AND USER2_ID=?";
            boolean exists2 = false;
            int count3 = jdbcTemplate.queryForObject(sqlQuery, new Object[]{userId, friendId}, Integer.class);
            int count4 = jdbcTemplate.queryForObject(sqlQuery, new Object[]{friendId, userId}, Integer.class);
            exists2 = count3 > 0 || count4 > 0;
            if (exists2 == false) {
                jdbcTemplate.update("INSERT INTO FRIENDSHIP (USER1_ID, USER2_ID, STATUS) VALUES (?, ?, ?)", friendId, userId, "unconfirmed");
            } else if (count3 > 0) {
                jdbcTemplate.update("UPDATE FRIENDSHIP SET STATUS = ? WHERE USER2_ID=? AND USER1_ID=?", "confirmed", friendId, userId);
            } else if (count4 > 0) {
                jdbcTemplate.update("UPDATE FRIENDSHIP SET STATUS = ? WHERE USER2_ID=? AND USER1_ID=?", "confirmed", userId, friendId);
            }
        }
    }

    @Override
    public void deleteFriend(long userId, long friendId) {
        String sql = "DELETE FROM FRIENDSHIP WHERE USER1_ID=? AND USER2_ID=?;";
        jdbcTemplate.update(sql, friendId, userId);
    }

    @Override
    public List<User> findAllFriends(long userId) {
        String sql = "SELECT * FROM USERS WHERE USER_ID IN (SELECT USER1_ID FROM FRIENDSHIP WHERE USER2_ID=?)";
        return jdbcTemplate.query(sql, new Object[]{userId}, userMapper);
    }

    @Override
    public List<User> findCommonFriends(long userId, long otherUserId) {
        String sql = "SELECT * FROM USERS WHERE USER_ID=" +
                "(SELECT * FROM(SELECT USER1_ID FROM FRIENDSHIP WHERE USER2_ID=? " +
                "UNION ALL SELECT USER1_ID FROM FRIENDSHIP WHERE USER2_ID=?) GROUP BY USER1_ID HAVING COUNT(USER1_ID)=2)";
        return jdbcTemplate.query(sql, new Object[]{userId, otherUserId}, userMapper);
    }

    @Override
    public List<Film> getUserRecommendations(int id) {
        HashMap<Integer, List<Film>> likesForUsers = getListLikesForUsers();
        List<Film> listUserLikeFilms = likesForUsers.get(id);
        likesForUsers.remove(id);
        HashMap<Integer, List<Film>> different = new HashMap<>(likesForUsers);
        for (List<Film> films : different.values()) {
            films.removeAll(listUserLikeFilms);
        }
        HashMap<Integer, List<Film>> identical = new HashMap<>(getListLikesForUsers());
        identical.remove(id);
        for (List<Film> films : identical.values()) {
            films.retainAll(listUserLikeFilms);
        }
        int maxSize = 0;
        int idMaxSize = -1;
        for (Map.Entry<Integer, List<Film>> films : identical.entrySet()) {
            if (films.getValue().size() > maxSize) {
                maxSize = films.getValue().size();
                idMaxSize = films.getKey();
            }
        }
        if (different.get(idMaxSize) == null) {
            return new ArrayList<>();
        }
        return new ArrayList<>(different.get(idMaxSize));
    }

    public HashMap<Integer, List<Film>> getListLikesForUsers() {
        HashMap<Integer, List<Film>> likesForUsers = new HashMap<>();
        List<User> userList = findAllUser();
        for (User user : userList) {
            String sqlQuery = "SELECT Films.FILM_ID, Films.FILM_NAME, Films.DESCRIPTION, Films.RELEASE_DATE, Films.DURATION, Films.MPA_ID " +
                    "FROM Films " +
                    "INNER JOIN LIKES ON Films.FILM_ID = LIKES.FILM_ID " +
                    "WHERE LIKES.USER_ID = " + user.getId() + " " +
                    "GROUP BY LIKES.FILM_ID ";
            List<Film> list = new ArrayList<>();
            List<Map<String, Object>> films = jdbcTemplate.queryForList(sqlQuery);
            films.forEach(rowMap -> list.add(
                    new Film((int) rowMap.get("FILM_ID"),
                            (String) rowMap.get("FILM_NAME"),
                            (String) rowMap.get("DESCRIPTION"),
                            LocalDate.parse(rowMap.get("RELEASE_DATE").toString()),
                            (int) rowMap.get("DURATION"),
                            new HashSet<>(filmDbStorage.getListGenresForFilmId((int) rowMap.get("FILM_ID"))),
                            mpaDbStorage.getFilmMpaById((int) rowMap.get("MPA_ID")))
            ));
            likesForUsers.put((int) user.getId(), list);
        }
        return likesForUsers;
    }
}
