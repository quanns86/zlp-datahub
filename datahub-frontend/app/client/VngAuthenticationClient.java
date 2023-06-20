package client;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.AllArgsConstructor;
import okhttp3.FormBody;
import okhttp3.Request;
import okhttp3.RequestBody;
import utils.ConfigUtil;
import utils.OkHttpRequester;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

public class VngAuthenticationClient {
    private final OkHttpRequester okHttpRequester = new OkHttpRequester();
    public static final String VNG_OTP_URL = "https://security.vng.com.vn/token-gateway/api/verify_otp";

    public Response authenticate(String username, String code) throws IOException {
        if (ConfigUtil.DEFAULT_GMS_ALLOWED_USERS.contains(username)) {
            RequestBody formBody = new FormBody.Builder()
                    .add("username", username)
                    .add("code", code)
                    .add("type", "ga")
                    .build();
            Request request = new Request.Builder().url(VNG_OTP_URL).post(formBody).build();
            return okHttpRequester.callSync(request, Response.class);
        }
        return new Response(false);
    }

    @Getter
    @Setter
    @ToString
    @AllArgsConstructor
    public static class Response {
        private Boolean status;
    }
}
