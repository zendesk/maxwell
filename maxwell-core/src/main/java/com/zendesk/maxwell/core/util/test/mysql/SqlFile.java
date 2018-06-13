package com.zendesk.maxwell.core.util.test.mysql;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class SqlFile {
    private final String fileName;
    private final byte[] data;

    SqlFile(String fileName, byte[] data) {
        this.fileName = fileName;
        this.data = data;
    }

    public String getFileName() {
        return fileName;
    }

    public byte[] getData() {
        return data;
    }

    public List<String> getDataLines() {
        List<String> lines = new ArrayList<>();
        if (data == null) {
            return lines;
        }

        try(BufferedReader r = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(data),StandardCharsets.UTF_8))) {
            for (String line = r.readLine(); line != null; line = r.readLine()) {
                lines.add(line);
            }
        } catch (IOException e) {
            throw new IllegalStateException("Cannot convert sql data to lines of strings", e);
        }

        return lines;
    }
}
