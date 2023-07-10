package com.enterprisewebservice;

import redis.clients.jedis.commands.ProtocolCommand;
import redis.clients.jedis.util.SafeEncoder;

public class FTCreateCommand implements ProtocolCommand {
    private final String command;

    public FTCreateCommand(String command) {
        this.command = command;
    }

    @Override
    public byte[] getRaw() {
        return SafeEncoder.encode(command);
    }
}

