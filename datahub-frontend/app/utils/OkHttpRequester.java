package utils;

import com.google.gson.JsonElement;
import okhttp3.*;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;

public class OkHttpRequester {
    private final OkHttpClient okHttpClient = new OkHttpClient();

    public <T> T callSync(Request request, Class<T> clazz) throws IOException {
        Call call = okHttpClient.newCall(request);
        try (Response response = call.execute()) {
            if (!response.isSuccessful()) {
                throw new IOException(String.valueOf(response.code()));
            }
            ResponseBody responseBody = response.body();
            if (responseBody != null) {
                String bodyString = responseBody.string();
                if (!StringUtils.isEmpty(bodyString)) {
                    return GsonUtils.parse(bodyString, clazz);
                }
            }
            return GsonUtils.parse(null, clazz);
        }
    }
}
