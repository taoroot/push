package cn.flizi.push.service.impl;

import cn.flizi.push.service.DeviceService;
import org.springframework.stereotype.Service;

@Service
public class DeviceServiceImpl implements DeviceService {
    @Override
    public Boolean auth(String username, String password) {
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return true;
    }
}
