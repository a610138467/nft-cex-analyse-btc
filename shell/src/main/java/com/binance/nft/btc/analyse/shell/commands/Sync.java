package com.binance.nft.btc.analyse.shell.commands;

import org.apache.kafka.common.protocol.types.Field;
import org.bouncycastle.jcajce.provider.digest.SHA256;
import org.springframework.beans.propertyeditors.CurrencyEditor;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.validation.annotation.Validated;
import org.springframework.shell.standard.ShellComponent;
import org.springframework.shell.standard.ShellMethod;
import org.springframework.web.bind.annotation.*;
import org.springframework.stereotype.Component;
import org.apache.commons.codec.binary.Hex;
import lombok.extern.slf4j.Slf4j;
import javax.annotation.Resource;
import io.swagger.annotations.*;
import org.bitcoinj.core.*;

import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.io.*;
import org.bitcoinj.params.RegTestParams;
import org.bitcoinj.utils.*;
import java.text.*;
import java.nio.ByteBuffer;
import java.io.FileReader;
import java.util.concurrent.*;

@Slf4j
@ShellComponent("sync")
public class Sync {

    public static class UtxoHashMapList {
        final int listSize = 0xff + 1;
        List<Map<Sha256Hash, Map<Integer, Long>>> hashMapList = new ArrayList<>();

        public UtxoHashMapList() {
            for (int i = 0; i < listSize; i ++) {
                hashMapList.add(new HashMap<>());
            }
        }

        public List<Map<Sha256Hash, Map<Integer, Long>>> getHashMapList() {
            return this.hashMapList;
        }

        public Long put(Sha256Hash hash, Integer index, Long value) {
            int listIndex = hash.getBytes()[31] & 0xff;
            Map<Sha256Hash, Map<Integer, Long>> map = hashMapList.get(listIndex);
            Map<Integer, Long> valueMap = map.computeIfAbsent(hash, k -> new HashMap<>());
            return valueMap.put(index, value);
        }

        public Long remove(Sha256Hash hash, Integer index) {
            int listIndex = hash.getBytes()[31] & 0xff;
            Map<Sha256Hash, Map<Integer, Long>> map = hashMapList.get(listIndex);
            Map<Integer, Long> valueMap = map.get(hash);
            if (valueMap == null) {
                return null;
            }
            Long res = valueMap.remove(index);
            if (valueMap.keySet().isEmpty()) {
                map.remove(hash);
            }
            return res;
        }

        public Long get(Sha256Hash hash, Integer index) {
            int listIndex = hash.getBytes()[31] & 0xff;
            Map<Sha256Hash, Map<Integer, Long>> map = hashMapList.get(listIndex);
            Map<Integer, Long> valueMap = map.get(hash);
            if (valueMap == null) {
                return null;
            }
            return valueMap.get(index);
        }
    }

    @ShellMethod("sync utxo")
    public void utxo(
            String blockHashFile, int startBlock, int endBlock,
            String blockDataDir, int startDataIndex, int endDataIndex,
            String prevUtxoFile, String outputFile, int threadNum
    ) throws IOException, InterruptedException, ExecutionException {
//    public static void main(String[] args) throws IOException, InterruptedException, ExecutionException {
//        String blockHashFile = "/Users/user/src/nft-cex-sync-btc/sync-job/block_id_to_hash.csv";
//        int startBlock = 0;
//        int endBlock = 500000;
//        String blockDataDir = "/Users/user/src/bitcoin/run/data/blocks";
//        int startDataIndex = 0;
//        int endDataIndex = 10000;
//        String prevUtxoFile = "./utxo.csv";
//        String outputFile = "./utxo_0_500000.csv";
//        Integer threadNum = 16;

        long startTime = System.currentTimeMillis();
        //获取所有要出里的的block data文件
        if (startDataIndex > endDataIndex) {
            log.error("illegal data index [{}-{}]", startDataIndex, endDataIndex);
            System.exit(-1);
        }
        Deque<File> blockDataFileList = new ArrayDeque<>();
        for (int index = startDataIndex; index <= endDataIndex; index ++) {
            File file = new File(String.format("%s/blk%05d.dat", blockDataDir, index));
            if (!file.exists()) {
                break;
            }
            blockDataFileList.add(file);
        }
        String startDataFile = blockDataFileList.getFirst().getName();
        String endDataFile = blockDataFileList.getLast().getName();
        log.info("deal block data: [{}-{}]", startDataFile, endDataFile);
        //从blockHash文件中获取所有的block height及对应的block hash
        if (!(new File(blockHashFile).exists())) {
            System.out.println(String.format("%s not exist", blockHashFile));
            System.exit(-1);
        }
        long loadBlockHashTime = System.currentTimeMillis();
        Map<String, Integer> blockHashToHeight = new HashMap<>();
        {
            BufferedReader blockHashFileReader = new BufferedReader(new FileReader(blockHashFile));
            String line = "";
            while((line = blockHashFileReader.readLine()) != null) {
                String[] items = line.split(",");
                Integer blockNumber = Integer.valueOf(items[0]);
                if (blockNumber > endBlock || blockNumber < startBlock) {
                    continue;
                }
                blockHashToHeight.put(items[1], Integer.valueOf(items[0]));
            }
            blockHashFileReader.close();
        }
        log.info("load block hash finish number: {}, cost: {} ms", blockHashToHeight.keySet().size(), System.currentTimeMillis() - loadBlockHashTime);
        //加载之前的utxo用于归并数据
        long loadPrevUtxoTime = System.currentTimeMillis();
        Map<String, Long> utxoMap = new HashMap<>();
        if ((new File(prevUtxoFile)).exists()) {
            BufferedReader prevUtxoFileReader = new BufferedReader(new FileReader(prevUtxoFile));
            String line = "";
            while((line = prevUtxoFileReader.readLine()) != null) {
                String[] items = line.split(",");
                utxoMap.put(String.format("%s,%s", items[0], items[1]), Long.valueOf(items[2]));
            }
        }
        log.info("load prev utxo data finish number: {}, cost {} ms", utxoMap.keySet().size(), System.currentTimeMillis() - loadPrevUtxoTime);
        //多线程同步数据
        {
            NetworkParameters networkParameters = NetworkParameters.fromID(NetworkParameters.ID_MAINNET);
            ExecutorService threadPool = Executors.newFixedThreadPool(threadNum);
            List<StringBuilder> stringBuilderList = new ArrayList<>(threadNum);
            List<HashMap<String, Long>> utxoMapList = new ArrayList<>(threadNum);
            for (int i = 0; i < threadNum; i++) {
                stringBuilderList.add(new StringBuilder(100));
                utxoMapList.add(new HashMap<>(100000));
            }
            while (!blockDataFileList.isEmpty()) {
                long loopStartTime = System.currentTimeMillis();
                int workerNum = Math.min(blockDataFileList.size(), threadNum);
                Deque<Map.Entry<Integer, File>> fileListForWorker = new ArrayDeque<>();
                for (int i = 0; i < workerNum; i++) {
                    fileListForWorker.add(new AbstractMap.SimpleEntry<Integer, File>(i, blockDataFileList.pop()));
                }
                CountDownLatch latch = new CountDownLatch(workerNum);
                List<Future<Map<String, Long>>> workerResultList = new ArrayList<>();
                for (Map.Entry<Integer, File> item : fileListForWorker) {
                    Future<Map<String, Long>> workerResult = threadPool.submit(
                            () -> {
                                File dataFile = item.getValue();
                                StringBuilder builder = stringBuilderList.get(item.getKey());
                                long workerStartTime = System.currentTimeMillis();
                                log.info("work for {} begin", dataFile.getName());
                                Map<String, Long> res = utxoMapList.get(item.getKey());
                                res.clear();
                                try {
                                    Context cx = Context.getOrCreate(networkParameters);
                                    Context.propagate(cx);
                                    BlockFileLoader loader = new BlockFileLoader(networkParameters, Arrays.asList(dataFile));
                                    for (Block block : loader) {
                                        Integer blockHeight = blockHashToHeight.get(block.getHashAsString());
                                        if (blockHeight == null) {
                                            continue;
                                        }
                                        if (blockHeight % 1000 == 0) {
                                            log.info("block height: {}", blockHeight);
                                        }
                                        for (Transaction transaction : Objects.requireNonNull(block.getTransactions())) {
                                            String txHash = transaction.getTxId().toString();
                                            if (!transaction.isCoinBase()) {
                                                for (TransactionInput input : transaction.getInputs()) {
//                                                String key = String.format("%s,%d", input.getOutpoint().getHash(), input.getOutpoint().getIndex());
                                                    builder.setLength(0);
                                                    builder.append(input.getOutpoint().getHash());
                                                    builder.append(",");
                                                    builder.append(input.getOutpoint().getIndex());
                                                    if (res.remove(builder.toString()) == null) {
                                                        res.put(builder.toString(), 0L);
                                                    }
                                                }
                                            }
                                            for (TransactionOutput output : transaction.getOutputs()) {
//                                            String key = String.format("%s,%d", txHash, output.getIndex());
                                                builder.setLength(0);
                                                builder.append(txHash);
                                                builder.append(",");
                                                builder.append(output.getIndex());
                                                if (res.remove(builder.toString()) == null) {
                                                    res.put(builder.toString(), output.getValue().toSat());
                                                }
                                            }
                                        }
                                        if (blockHeight % 1000 == 0) {
                                            log.info("process finish: {}", blockHeight);
                                        }
                                    }
                                    log.info("work for {} finish size: {}, cost: {} ms",
                                            dataFile.getName(), res.keySet().size(), System.currentTimeMillis() - workerStartTime
                                    );
                                } catch (Exception ex) {
                                    log.error("caught exception: ", ex);
                                } finally {
                                    latch.countDown();
                                }
                                return res;
                            }
                    );
                    workerResultList.add(workerResult);
                }
                latch.await();
                for (Future<Map<String, Long>> result : workerResultList) {
                    Map<String, Long> res = result.get();
                    for (Map.Entry<String, Long> item : res.entrySet()) {
                        if (utxoMap.remove(item.getKey()) == null) {
                            utxoMap.put(item.getKey(), item.getValue());
                        }
                    }
                }
                log.info("scan block data [{},{}] finish cost {} ms remain {} items utxo size: {}",
                        fileListForWorker.getFirst().getValue().getName(),
                        fileListForWorker.getLast().getValue().getName(),
                        System.currentTimeMillis() - loopStartTime,
                        blockDataFileList.size(),
                        utxoMap.keySet().size()
                );
            }
        }
        long saveTime = System.currentTimeMillis();
        {
            BufferedWriter outputFileWriter = new BufferedWriter(new FileWriter(outputFile), 16 * 1024 * 1024);
            for (Map.Entry<String, Long> item : utxoMap.entrySet()) {
                outputFileWriter.write(String.format("%s,%d\n", item.getKey(), item.getValue()));
            }
            outputFileWriter.flush();
            outputFileWriter.close();
        }
        log.info("save utxo to file finish. number: {}, cost: {} ms", utxoMap.keySet().size(), System.currentTimeMillis() - saveTime);

        log.info("sync utxo [{},{}] finish, utxo num: {}, total cost: {} result file: {}",
                startBlock, endBlock, utxoMap.keySet().size(), System.currentTimeMillis() - startTime, outputFile
        );
    }

    @ShellMethod("sync utxo")
    public void utxo2(
            String blockHashFile, int startBlock, int endBlock,
            String blockDataDir, int startDataIndex, int endDataIndex,
            String prevUtxoFile, String outputFile, int threadNum
    ) throws IOException, InterruptedException, ExecutionException {
//    public static void main(String[] args) throws IOException, InterruptedException, ExecutionException {
//        String blockHashFile = "/Users/user/src/nft-cex-sync-btc/sync-job/block_id_to_hash.csv";
//        int startBlock = 0;
//        int endBlock = 500000;
//        String blockDataDir = "/Users/user/src/bitcoin/run/data/blocks";
//        int startDataIndex = 0;
//        int endDataIndex = 10000;
//        String prevUtxoFile = "./utxo.csv";
//        String outputFile = "./utxo_0_500000.csv";
//        Integer threadNum = 16;

        long startTime = System.currentTimeMillis();
        //获取所有要出里的的block data文件
        if (startDataIndex > endDataIndex) {
            log.error("illegal data index [{}-{}]", startDataIndex, endDataIndex);
            System.exit(-1);
        }
        Deque<File> blockDataFileList = new ArrayDeque<>();
        for (int index = startDataIndex; index <= endDataIndex; index ++) {
            File file = new File(String.format("%s/blk%05d.dat", blockDataDir, index));
            if (!file.exists()) {
                break;
            }
            blockDataFileList.add(file);
        }
        String startDataFile = blockDataFileList.getFirst().getName();
        String endDataFile = blockDataFileList.getLast().getName();
        log.info("deal block data: [{}-{}]", startDataFile, endDataFile);
        //从blockHash文件中获取所有的block height及对应的block hash
        if (!(new File(blockHashFile).exists())) {
            System.out.println(String.format("%s not exist", blockHashFile));
            System.exit(-1);
        }
        long loadBlockHashTime = System.currentTimeMillis();
        Map<String, Integer> blockHashToHeight = new HashMap<>();
        {
            BufferedReader blockHashFileReader = new BufferedReader(new FileReader(blockHashFile));
            String line = "";
            while((line = blockHashFileReader.readLine()) != null) {
                String[] items = line.split(",");
                int blockNumber = Integer.parseInt(items[0]);
                if (blockNumber > endBlock || blockNumber < startBlock) {
                    continue;
                }
                blockHashToHeight.put(items[1], Integer.valueOf(items[0]));
            }
            blockHashFileReader.close();
        }
        log.info("load block hash finish number: {}, cost: {} ms", blockHashToHeight.keySet().size(), System.currentTimeMillis() - loadBlockHashTime);
        //加载之前的utxo用于归并数据
        long loadPrevUtxoTime = System.currentTimeMillis();
//        Map<String, Long> utxoMap = new HashMap<>();
        UtxoHashMapList utxoMap = new UtxoHashMapList();
        if ((new File(prevUtxoFile)).exists()) {
            BufferedReader prevUtxoFileReader = new BufferedReader(new FileReader(prevUtxoFile));
            String line = "";
            while((line = prevUtxoFileReader.readLine()) != null) {
                String[] items = line.split(",");
                Sha256Hash hash = Sha256Hash.wrap(Utils.HEX.decode(items[0]));
                utxoMap.put(hash, Integer.valueOf(items[1]), Long.valueOf(items[2]));
            }
        }
        log.info("load prev utxo data finish number: {}, cost {} ms", "e", System.currentTimeMillis() - loadPrevUtxoTime);
        //多线程同步数据
        {
            NetworkParameters networkParameters = NetworkParameters.fromID(NetworkParameters.ID_MAINNET);
            ExecutorService threadPool = Executors.newFixedThreadPool(threadNum);
            List<StringBuilder> stringBuilderList = new ArrayList<>(threadNum);
//            List<HashMap<String, Long>> utxoMapList = new ArrayList<>(threadNum);
            for (int i = 0; i < threadNum; i++) {
                stringBuilderList.add(new StringBuilder(100));
//                utxoMapList.add(new HashMap<>(100000));
            }
            while (!blockDataFileList.isEmpty()) {
                long loopStartTime = System.currentTimeMillis();
                int workerNum = Math.min(blockDataFileList.size(), threadNum);
                Deque<Map.Entry<Integer, File>> fileListForWorker = new ArrayDeque<>();
                for (int i = 0; i < workerNum; i++) {
                    fileListForWorker.add(new AbstractMap.SimpleEntry<Integer, File>(i, blockDataFileList.pop()));
                }
                CountDownLatch latch = new CountDownLatch(workerNum);
                List<Future<UtxoHashMapList>> workerResultList = new ArrayList<>();
                for (Map.Entry<Integer, File> item : fileListForWorker) {
                    Future<UtxoHashMapList> workerResult = threadPool.submit(
                            () -> {
                                File dataFile = item.getValue();
                                StringBuilder builder = stringBuilderList.get(item.getKey());
                                long workerStartTime = System.currentTimeMillis();
                                log.info("work for {} begin", dataFile.getName());
                                UtxoHashMapList res = new UtxoHashMapList();
//                                res.clear();
                                try {
                                    Context cx = Context.getOrCreate(networkParameters);
                                    Context.propagate(cx);
                                    BlockFileLoader loader = new BlockFileLoader(networkParameters, Arrays.asList(dataFile));
                                    for (Block block : loader) {
                                        Integer blockHeight = blockHashToHeight.get(block.getHashAsString());
                                        if (blockHeight == null) {
                                            continue;
                                        }
                                        if (blockHeight % 1000 == 0) {
                                            log.info("block height: {}", blockHeight);
                                        }
                                        for (Transaction transaction : Objects.requireNonNull(block.getTransactions())) {
                                            String txHash = transaction.getTxId().toString();
                                            if (!transaction.isCoinBase()) {
                                                for (TransactionInput input : transaction.getInputs()) {
//                                                String key = String.format("%s,%d", input.getOutpoint().getHash(), input.getOutpoint().getIndex());
//                                                    builder.setLength(0);
//                                                    builder.append(input.getOutpoint().getHash());
//                                                    builder.append(",");
//                                                    builder.append(input.getOutpoint().getIndex());
                                                    if (res.remove(input.getOutpoint().getHash(), (int)input.getOutpoint().getIndex()) == null) {
                                                        res.put(input.getOutpoint().getHash(), (int)input.getOutpoint().getIndex(), 0L);
                                                    }
                                                }
                                            }
                                            for (TransactionOutput output : transaction.getOutputs()) {
//                                            String key = String.format("%s,%d", txHash, output.getIndex());
                                                builder.setLength(0);
                                                builder.append(txHash);
                                                builder.append(",");
                                                builder.append(output.getIndex());
                                                if (res.remove(transaction.getTxId(), output.getIndex()) == null) {
//                                                    res.put(builder.toString(), output.getValue().toSat());
                                                    res.put(transaction.getTxId(), output.getIndex(), output.getValue().toSat());
                                                }
                                            }
                                        }
                                        if (blockHeight % 1000 == 0) {
                                            log.info("process finish: {}", blockHeight);
                                        }
                                    }
                                    log.info("work for {} finish size: {}, cost: {} ms",
                                            dataFile.getName(), "e", System.currentTimeMillis() - workerStartTime
                                    );
                                } catch (Exception ex) {
                                    log.error("caught exception: ", ex);
                                } finally {
                                    latch.countDown();
                                }
                                return res;
                            }
                    );
                    workerResultList.add(workerResult);
                }
                latch.await();
                int totalSize = 0;
                for (Future<UtxoHashMapList> result : workerResultList) {
                    UtxoHashMapList res = result.get();
                    for (Map<Sha256Hash, Map<Integer, Long>> item : res.getHashMapList()) {
                        for (Map.Entry<Sha256Hash, Map<Integer, Long>> tx : item.entrySet()) {
                            Sha256Hash txHash = tx.getKey();
                            for (Map.Entry<Integer, Long> vout : tx.getValue().entrySet()) {
                                if (utxoMap.remove(txHash, vout.getKey()) == null) {
                                    utxoMap.put(txHash, vout.getKey(), vout.getValue());
                                    totalSize += 1;
                                }
                            }
                        }
                    }
                }
                log.info("scan block data [{},{}] finish cost {} ms remain {} items utxo size: {}",
                        fileListForWorker.getFirst().getValue().getName(),
                        fileListForWorker.getLast().getValue().getName(),
                        System.currentTimeMillis() - loopStartTime,
                        blockDataFileList.size(),
                        totalSize
                );
            }
        }
        long saveTime = System.currentTimeMillis();
        int totalNum = 0;
        {
            BufferedWriter outputFileWriter = new BufferedWriter(new FileWriter(outputFile), 16 * 1024 * 1024);
            for (Map<Sha256Hash, Map<Integer, Long>> item : utxoMap.getHashMapList()) {
                for (Map.Entry<Sha256Hash, Map<Integer, Long>> tx : item.entrySet()) {
                    Sha256Hash txHash = tx.getKey();
                    for (Map.Entry<Integer, Long> vout : tx.getValue().entrySet()) {
                        outputFileWriter.write(String.format("%s,%d,%d\n", Utils.HEX.encode(txHash.getBytes()), vout.getKey(), vout.getValue()));
                        totalNum ++;
                    }
                }
            }
            outputFileWriter.flush();
            outputFileWriter.close();
        }
        log.info("save utxo to file finish. number: {}, cost: {} ms", totalNum, System.currentTimeMillis() - saveTime);

        log.info("sync utxo [{},{}] finish, utxo num: {}, total cost: {} result file: {}",
                startBlock, endBlock, totalNum, System.currentTimeMillis() - startTime, outputFile
        );
    }

    @ShellMethod("load data file")
    public void load (
            String blockHashFile, int startBlock, int endBlock,
            String blockDataDir, int startDataIndex, int endDataIndex,
            String outputDir, int threadNum
    ) throws IOException, InterruptedException, ExecutionException {
        if (!(new File(blockDataDir).exists() || !(new File(blockDataDir)).isDirectory())) {
            log.info("block data dir {} not exist", outputDir);
            System.exit(-1);
        }
        Deque<File> blockDataFileList = new ArrayDeque<>();
        for (int index = startDataIndex; index <= endDataIndex; index ++) {
            File file = new File(String.format("%s/blk%05d.dat", blockDataDir, index));
            if (!file.exists()) {
                break;
            }
            blockDataFileList.add(file);
        }

        Map<String, Integer> blockHashToHeight = new HashMap<>();
        {
            BufferedReader blockHashFileReader = new BufferedReader(new FileReader(blockHashFile));
            String line = "";
            while((line = blockHashFileReader.readLine()) != null) {
                String[] items = line.split(",");
                Integer blockNumber = Integer.valueOf(items[0]);
                if (blockNumber > endBlock || blockNumber < startBlock) {
                    continue;
                }
                blockHashToHeight.put(items[1], Integer.valueOf(items[0]));
            }
            blockHashFileReader.close();
        }

        if (!(new File(outputDir).exists() || !(new File(outputDir)).isDirectory())) {
            log.info("data dir {} not exist", outputDir);
            System.exit(-1);
        }

        ExecutorService threadPool = Executors.newFixedThreadPool(threadNum);

        NetworkParameters networkParameters = NetworkParameters.fromID(NetworkParameters.ID_MAINNET);
        while (!blockDataFileList.isEmpty()) {
            long loopStartTime = System.currentTimeMillis();
            int workerNum = Math.min(blockDataFileList.size(), threadNum);
            List<File> fileListForWorker = new ArrayList<>();
            for (int i = 0; i < workerNum; i++) {
                fileListForWorker.add(blockDataFileList.pop());
            }
            CountDownLatch latch = new CountDownLatch(workerNum);
            Integer maxBlockHeight = 0;
            List<Future<Integer>>  workerResultList = new ArrayList<>();
            for (File dataFile : fileListForWorker) {
                Future<Integer> workerMaxBlockNumber = threadPool.submit(
                        () -> {
                            Integer height = 0;
                            try {
                                String inputFile = String.format("%s/input.%s.dat", outputDir, dataFile.getName());
                                DataOutputStream inputDos = new DataOutputStream(
                                        new BufferedOutputStream(
                                                Files.newOutputStream(Paths.get(inputFile))
                                        )
                                );
                                String outputFile = String.format("%s/output.%s.dat", outputDir, dataFile.getName());
                                DataOutputStream outputDos = new DataOutputStream(
                                        new BufferedOutputStream(
                                            Files.newOutputStream(Paths.get(outputFile))
                                        )
                                );
                                Context cx = Context.getOrCreate(networkParameters);
                                BlockFileLoader loader = new BlockFileLoader(networkParameters, Arrays.asList(dataFile));
                                for (Block block : loader) {
                                    Integer blockHeight = blockHashToHeight.get(block.getHashAsString());
                                    if (blockHeight == null) {
                                        continue;
                                    }
                                    if (blockHeight > height) {
                                        height = blockHeight;
                                    }
                                    for (Transaction transaction : Objects.requireNonNull(block.getTransactions())) {
                                        if (!transaction.isCoinBase()) {
                                            for (TransactionInput input : transaction.getInputs()) {
                                                inputDos.write(input.getOutpoint().getHash().getBytes(), 0, 32);
                                                inputDos.writeInt((int) input.getOutpoint().getIndex());
                                            }
                                        }
                                        for (TransactionOutput output : transaction.getOutputs()) {
                                            outputDos.write(transaction.getTxId().getBytes(), 0, 32);
                                            outputDos.writeInt(output.getIndex());
                                            outputDos.writeLong(output.getValue().toSat());
                                        }
                                    }
                                }
                                inputDos.flush();
                                inputDos.close();
                                outputDos.flush();
                                outputDos.close();
                            } catch (Exception ex) {
                                log.error("caught ex: ", ex);
                            } finally {
                                latch.countDown();
                            }
                            return height;
                        }
                );
                workerResultList.add(workerMaxBlockNumber);
            }
            latch.await();
            for(Future<Integer> item : workerResultList) {
                if (item.get() > maxBlockHeight) {
                    maxBlockHeight = item.get();
                }
            }
            log.info("save [{}-{}] finish cost: {} ms currentMaxBlockHeight: {}",
                    fileListForWorker.get(0).getName(),
                    fileListForWorker.get(fileListForWorker.size() - 1).getName(),
                    System.currentTimeMillis() - loopStartTime,
                    maxBlockHeight
            );
        }
    }

    public static class InputItem {
        Sha256Hash txHash;
        int voutIndex;
    }

    @ShellMethod("combin utxo")
    public void combin (
            String prevUtxoFile, String outputFileDir, int startIndex, int endIndex, String outputFilePath
    ) throws IOException, InterruptedException, ExecutionException {
//    public static void main(String[] args) throws IOException, InterruptedException, ExecutionException {
//        String prevUtxoFile = "./utxo1.csv";
//        String outputFileDir = "/Users/user/src/nft-cex-analyse-btc/outputs";
//        String outputFilePath = "./utxo.csv";
//        int startIndex = 0;
//        int endIndex = 0;
        UtxoHashMapList utxoMap = new UtxoHashMapList();
        long loadPrevUtxoStartTime = System.currentTimeMillis();
//        byte[] hashBytes = new byte[32];
        int prevUtxoNum = 0;
        if ((new File(prevUtxoFile)).exists()) {
            DataInputStream outputDis = new DataInputStream(
                    new BufferedInputStream(
                        Files.newInputStream(Paths.get(prevUtxoFile))
                    )
            );
            while(outputDis.available() > 0) {
                byte[] hashBytes = new byte[32];
                outputDis.read(hashBytes, 0, 32);
                Sha256Hash hash = Sha256Hash.wrap(hashBytes);
                Integer index = outputDis.readInt();
                Long value = outputDis.readLong();
                utxoMap.put(hash, index, value);
                prevUtxoNum ++;
            }
            outputDis.close();
        }
        log.info("load prev utxo file finish. cost {} ms utxo num: {}", System.currentTimeMillis() - loadPrevUtxoStartTime, prevUtxoNum);

        File outputFileDirFile = new File(outputFileDir);
        if (!outputFileDirFile.exists() || !outputFileDirFile.isDirectory()) {
            log.error("illegal output file dir: {}", outputFileDir);
            System.exit(-1);
        }

        List<File> inputFileList = new ArrayList<>();
        List<File> outputFileList = new ArrayList<>();
        for (int i = startIndex; i <= endIndex; i ++) {
            long loopStartTime = System.currentTimeMillis();
            File inputFile = new File(String.format("%s/input.blk%05d.dat.dat", outputFileDir, i));
            File outputFile = new File(String.format("%s/output.blk%05d.dat.dat", outputFileDir, i));
            if ((inputFile.exists() && !outputFile.exists() || (!inputFile.exists()) && outputFile.exists())) {
                log.error("file not exists: {} {}", inputFile.getName(), outputFile.getName());
            }
            if (!inputFile.exists() && !outputFile.exists()) {
                break;
            }
            {
                DataInputStream outputFileDis = new DataInputStream(
                        new BufferedInputStream(
                            Files.newInputStream(Paths.get(outputFile.getAbsolutePath())), 1024 * 1024 * 20
                        )
                );
                while (outputFileDis.available() > 0) {
                    byte[] hashBytes = new byte[32];
                    outputFileDis.read(hashBytes, 0, 32);
                    Sha256Hash hash = Sha256Hash.wrap(hashBytes);
                    Integer index = outputFileDis.readInt();
                    Long value = outputFileDis.readLong();
                    if (utxoMap.put(hash, index, value) != null) {
                        utxoMap.remove(hash, index);
                    }
                }
                outputFileDis.close();
            }

            {
                DataInputStream inputFileDis = new DataInputStream(
                        new BufferedInputStream(
                            Files.newInputStream(Paths.get(inputFile.getAbsolutePath())), 1024 * 1024 * 20
                        )
                );
                while (inputFileDis.available() > 0) {
                    byte[] hashBytes = new byte[32];
                    inputFileDis.read(hashBytes, 0, 32);
                    Sha256Hash hash = Sha256Hash.wrap(hashBytes);
                    Integer index = inputFileDis.readInt();
                    if (utxoMap.remove(hash, index) == null) {
                        utxoMap.put(hash, index, (long) (i + 1));
                    }
                }
                inputFileDis.close();
            }
            log.info("combin {} and {} finish. cost {} ms", inputFile.getName(), outputFile.getName(), System.currentTimeMillis() - loopStartTime);
        }
        {
            long saveStartTime = System.currentTimeMillis();
            DataOutputStream utxoOutput = new DataOutputStream(
                    new BufferedOutputStream(
                        Files.newOutputStream(Paths.get(outputFilePath))
                    )
            );
            int totalNum = 0;
            List<Map<Sha256Hash, Map<Integer, Long>>> mapList = utxoMap.getHashMapList();
//            Sha256Hash a = Sha256Hash.wrap(Utils.HEX.decode("e39d1473d6be4f72bb203e442e997abb95118218eba29d8e0faac173c9023900"));
//            Long res = utxoMap.get(a, 1);
            for (Map<Sha256Hash, Map<Integer, Long>> map : mapList) {
                for(Map.Entry<Sha256Hash, Map<Integer, Long>> item : map.entrySet()) {
                    Sha256Hash hash = item.getKey();
                    for (Map.Entry<Integer, Long> vout : item.getValue().entrySet()) {
                        utxoOutput.write(hash.getBytes(), 0, 32);
                        utxoOutput.writeInt(vout.getKey());
                        utxoOutput.writeLong(vout.getValue());
                        totalNum += 1;
                    }
                }
            }
            utxoOutput.flush();
            utxoOutput.close();
            log.info("save utxo file {} finish. cost {} ms, utxo size: {}", outputFilePath, System.currentTimeMillis() - saveStartTime, totalNum);
        }
        log.info("combin finish. [{},{}], cost: {} ms", startIndex, endIndex, System.currentTimeMillis() - loadPrevUtxoStartTime);
    }

    @ShellMethod("save to db")
    public void save (
            String utxoFilePath, String outputDirPath
    ) throws IOException {
//    public static void main(String[] args) throws IOException {
//        String utxoFilePath = "/Users/user/src/nft-cex-analyse-btc/utxo_0_100.csv";
//        String outputDirPath = "/Users/user/src/nft-cex-analyse-btc/sqls";
        long startTime = System.currentTimeMillis();
        if (!(new File(utxoFilePath)).exists()) {
            log.error("utxo file {} not exists", utxoFilePath);
            System.exit(-1);
        }
        if (!(new File(outputDirPath)).exists() || !(new File(outputDirPath)).isDirectory()) {
            log.error("output dir {} not exists or not directory", outputDirPath);
            System.exit(-1);
        }
        final BigInteger tableNumber = BigInteger.valueOf(32);
        final String sqlPrefix = "INSERT INTO vout_global_%d(`address`, `tx_hash`, `vout_index`, `amount`, `block_id`, `block_hash`, `block_state`, `is_whitelist`) values";
        final int bulkNum = 10000;
        List<StringBuilder> stringBuilderList = new ArrayList<>();
        List<String> sqlPrefixList = new ArrayList<>();
        List<Integer> valueNumberList = new ArrayList<>();
        List<BufferedOutputStream> outputStreamList = new ArrayList<>();
        for(int i = 0; i < tableNumber.intValue(); i ++) {
            sqlPrefixList.add(String.format(sqlPrefix, i));
            StringBuilder stringBuilder = new StringBuilder(1024 * 1024 * 8);
            stringBuilder.append(sqlPrefixList.get(i));
            stringBuilderList.add(stringBuilder);
            valueNumberList.add(0);
            outputStreamList.add(
                    new BufferedOutputStream(
                            Files.newOutputStream(
                                    new File(String.format("%s/vout_global_%d.sql", outputDirPath, i)).toPath()
                            )
                    )
            );
        }
        DataInputStream utxoFileDis = new DataInputStream(
                new BufferedInputStream(
                        Files.newInputStream(Paths.get(utxoFilePath)), 1024 * 1024 * 20
                )
        );
        byte[] hashBytes = new byte[32];
        int totalSaveNum = 0;
        while (utxoFileDis.available() > 0) {
            utxoFileDis.read(hashBytes, 0, 32);
            BigInteger hash = new BigInteger(hashBytes);
            int index = utxoFileDis.readInt();
            long value = utxoFileDis.readLong();
            int table = hash.mod(tableNumber).intValue();
            StringBuilder stringBuilder = stringBuilderList.get(table);
            stringBuilder.append(" ('','");
            stringBuilder.append(Utils.HEX.encode(hashBytes));
            stringBuilder.append("',");
            stringBuilder.append(index);
            stringBuilder.append(",");
            stringBuilder.append(Coin.ofSat(value).toBtc().toPlainString());
            stringBuilder.append(",0,'',2,0),");
            valueNumberList.set(table, valueNumberList.get(table) + 1);
            totalSaveNum += 1;
            for (int i = 0; i < tableNumber.intValue(); i ++) {
                if (valueNumberList.get(i) >= bulkNum) {
                    StringBuilder stringBuilderToUpdate = stringBuilderList.get(i);
                    stringBuilderToUpdate.delete(stringBuilderToUpdate.length() - 1, stringBuilderToUpdate.length());
                    stringBuilderToUpdate.append(";\n");
                    String sql = stringBuilderToUpdate.toString();
                    outputStreamList.get(i).write(sql.getBytes());
                    stringBuilderToUpdate.setLength(0);
                    stringBuilderToUpdate.append(sqlPrefixList.get(i));
                    log.info("save {} items sql into vout_global_{} finish current num: {}", valueNumberList.get(i), i, totalSaveNum);
                    valueNumberList.set(i, 0);
                }
            }
        }
        for (int i = 0; i < tableNumber.intValue(); i ++) {
            StringBuilder stringBuilderToUpdate = stringBuilderList.get(i);
            if (stringBuilderToUpdate.length() > sqlPrefixList.get(i).length()) {
                stringBuilderToUpdate.delete(stringBuilderToUpdate.length() - 1, stringBuilderToUpdate.length());
                stringBuilderToUpdate.append(";\n");
                String sql = stringBuilderToUpdate.toString();
                outputStreamList.get(i).write(sql.getBytes());
                stringBuilderToUpdate.setLength(0);
                stringBuilderToUpdate.append(sqlPrefixList.get(i));
                log.info("save {} items sql into vout_global_{} finish current num: {}", valueNumberList.get(i), i, totalSaveNum);
                valueNumberList.set(i, 0);
            }
            outputStreamList.get(i).flush();
            outputStreamList.get(i).close();
        }
        log.info("sava finish: total number: {}, cost: {} ms", totalSaveNum, System.currentTimeMillis() - startTime);
    }

    @ShellMethod("show utxo data")
    public void show (
            String utxoFilePath
    ) throws IOException {
//    public static void main(String[] args) throws IOException {
//        String utxoFilePath = "/Users/user/src/nft-cex-analyse-btc/utxo_0_300.csv";
        if (!(new File(utxoFilePath)).exists()) {
            log.error("utxo file {} not exists", utxoFilePath);
            System.exit(-1);
        }
        DataInputStream utxoFileDis = new DataInputStream(
                new BufferedInputStream(
                        Files.newInputStream(Paths.get(utxoFilePath)), 1024 * 1024 * 20
                )
        );
        while(utxoFileDis.available() > 0) {
            byte[] hashBytes = new byte[32];
            utxoFileDis.read(hashBytes, 0, 32);
            String hash = Utils.HEX.encode(hashBytes);
            Integer index = utxoFileDis.readInt();
            Long value = utxoFileDis.readLong();
            System.out.println(hash + "," + index + "," + value);
        }

    }

}
