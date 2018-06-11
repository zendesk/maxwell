package com.zendesk.maxwell.test.mysql;

import org.springframework.stereotype.Service;

import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.nio.file.*;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

@Service
public class MysqlTestData {
    public SqlFile getSqlFile(String relativeFileName){
        return readSqlTestResource(relativeFileName, p -> new SqlFile(p.getFileName().toString(), readFile(p)));
    }

    public List<SqlFile> getSqlFilesOfFolder(String subFolder){
        return getSqlFilesOfFolder(subFolder, null);
    }

    public List<SqlFile> getSqlFilesOfFolder(String resourceSubPath, Predicate<Path> filter){
        return readSqlTestResource(resourceSubPath, (p) -> getSqlFilesOfFolder(p, filter));
    }

    private <R> R readSqlTestResource(String resourceSubPath, Function<Path,R> processingFunction){
        try {
            final String resourcePath = "test-sql/"+resourceSubPath;
            URL resource = MysqlIsolatedServerSupport.class.getResource(resourcePath);
            resource = resource != null ? resource : MysqlIsolatedServerSupport.class.getClassLoader().getResource(resourcePath);
            if(resource == null){
                throw new IllegalStateException("Failed to load SQL resource " + resourceSubPath);
            }
            URI uri = resource.toURI();
            if (uri.getScheme().equals("jar")) {
                try(FileSystem fileSystem = FileSystems.newFileSystem(uri, Collections.<String, Object>emptyMap())){
                    Path path = fileSystem.getPath(resourcePath);
                    return processingFunction.apply(path);
                }
            } else {
                return processingFunction.apply(Paths.get(uri));
            }
        } catch (Exception e){
            throw new IllegalStateException("Failed to read SQL resource "+ resourceSubPath, e);
        }
    }

    private List<SqlFile> getSqlFilesOfFolder(Path path, Predicate<Path> filter) {
        Predicate<? super Path> combinedFilter = filter != null ? filter.and(Files::isRegularFile) : Files::isRegularFile;
        try {
            return Files.walk(path).filter(combinedFilter).map(p -> new SqlFile(p.getFileName().toString(), readFile(p))).collect(Collectors.toList());
        } catch (IOException e) {
            throw new IllegalStateException("Failed to read SQL files of folder "+path.getFileName().toString(), e);
        }
    }

    private byte[] readFile(Path p){
        try {
            return Files.readAllBytes(p);
        }catch(IOException e){
            throw new IllegalStateException("Cannot read SQL file "+ p.getFileName().toString(), e);
        }
    }
}
