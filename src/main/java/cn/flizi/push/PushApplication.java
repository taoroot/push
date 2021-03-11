package cn.flizi.push;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class PushApplication implements CommandLineRunner {
    private static final Logger logger = LogManager.getLogger(PushApplication.class);

    public static void main(String[] args) {
        SpringApplication.run(PushApplication.class, args);
    }

    @Override
    public void run(String... args) throws Exception {
//        DingTalkUtil.sendTextAsync(PushApplication.class.getSimpleName() + ":服务启动成功");
    }
}
