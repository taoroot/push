package cn.flizi.push.web;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class RestApiController {

    @GetMapping("/user_info")
    public String index() {
        return "/hello";
    }
}
