package utils;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class GsonUtils {
    public static final Gson GSON = new GsonBuilder().create();

    public static <T> T parse(String json, Class<T> clazz) {
        return GSON.fromJson(json, clazz);
    }
}
