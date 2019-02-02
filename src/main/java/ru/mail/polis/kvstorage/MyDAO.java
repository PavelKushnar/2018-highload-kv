package ru.mail.polis.kvstorage;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.util.NoSuchElementException;

import org.jetbrains.annotations.NotNull;

import ru.mail.polis.KVDao;

public class MyDAO implements KVDao{

    // единственная нетривиальная проблема - преобразовать набор байт - ключ в строку для имени файла
    // это разрешенные в имени файла символы, их 64 штуки (2^6)
    // то есть любой байт можно перевести в 2 таких символа // закодировать (2^8 == 2^6 * 2^2)
    private byte[] ALLOWS_SYMBOLS_64 = "qwertyuiopasdfghjklzxcvbnmQWERTYUIOPASDFGHJKLZXCVBNM1234567890-_".getBytes();

    private File storageFile;

    public MyDAO(File file){
        storageFile = file;
    }

    private String encode(byte[] data){
        byte[] newData = new byte[data.length * 2];
        int index = 0;

        for (byte dataElement : data){
            // приводим к 0-255
            int indexes = dataElement + 128;
            int firstIndex = indexes & 63; // 111111
            int secondIndex = (indexes & 192) >> 6; // 11000000 -> 6

            newData[index++] = ALLOWS_SYMBOLS_64[firstIndex];
            newData[index++] = ALLOWS_SYMBOLS_64[secondIndex];
        }

        return new String(newData);
    }

    private File getFile(byte[] key) {
        return new File(storageFile, encode(key));
    }

    @NotNull
    @Override
    public byte[] get(@NotNull byte[] key) throws NoSuchElementException, IOException {
        byte[] data;
        try {
            data = Files.readAllBytes(getFile(key).toPath());
        } catch (NoSuchFileException e) {
            throw new NoSuchElementException();
        }
        return data;
    }

    @Override
    public void upsert(@NotNull byte[] key, @NotNull byte[] value) throws IOException {
        Files.write(getFile(key).toPath(), value);
    }

    @Override
    public void remove(@NotNull byte[] key) throws IOException {
        try {
            Files.delete(getFile(key).toPath());
        } catch (NoSuchFileException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void close() throws IOException {

    }
}
