package ru.yandex.practicum.filmorate.model;


import lombok.Data;

import java.util.Objects;

@Data
public class Genre {
    long id;
    String name;

    public Genre() {
        }

    public Genre(long id, String name) {
        this.id = id;
        this.name = name;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Genre genre = (Genre) o;
        return id == genre.id;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }
}
