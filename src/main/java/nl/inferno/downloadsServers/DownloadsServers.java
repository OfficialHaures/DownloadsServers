package nl.inferno.downloadsServers;

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import org.bukkit.command.Command;
import org.bukkit.command.CommandExecutor;
import org.bukkit.command.CommandSender;
import org.bukkit.plugin.Plugin;
import org.bukkit.plugin.java.JavaPlugin;

public class BackupPlugin extends JavaPlugin implements CommandExecutor {
    private String dbHost;

    private String dbPort;

    private String dbUsername;

    private String dbPassword;

    private String dbName;

    private String azureConnectionString;

    private String azureContainerName;

    public void onEnable() {
        getCommand("movearmorstand").setExecutor(this);
        getLogger().info("MyMinecraftPlugin is ingeschakeld!");
        saveDefaultConfig();
        loadConfig();
    }

    public void onDisable() {
        getLogger().info("MyMinecraftPlugin is uitgeschakeld!");
    }

    private void loadConfig() {
        this.dbHost = getConfig().getString("database.host");
        this.dbPort = String.valueOf(getConfig().getInt("database.port"));
        this.dbUsername = getConfig().getString("database.username");
        this.dbPassword = getConfig().getString("database.password");
        this.dbName = getConfig().getString("database.database");
        this.azureConnectionString = getConfig().getString("azure.connection-string");
        this.azureContainerName = getConfig().getString("azure.container-name");
    }

    public boolean onCommand(CommandSender sender, Command command, String label, String[] args) {
        if (command.getName().equalsIgnoreCase("backupserver")) {
            sender.sendMessage("Backup proces gestart...");
            getLogger().info("Backup proces gestart door: " + sender.getName());
            getServer().getScheduler().runTaskAsynchronously((Plugin)this, () -> {
                try {
                    String dumpPath = exportAllDatabases();
                    sender.sendMessage("Databases genaar: " + dumpPath);
                    getLogger().info("Databases genaar: " + dumpPath);
                    String zipPath = zipFile(dumpPath);
                    sender.sendMessage("Dump gecomprimeerd naar: " + zipPath);
                    getLogger().info("Dump gecomprimeerd naar: " + zipPath);
                    boolean uploadSuccess = uploadToAzure(zipPath);
                    if (uploadSuccess) {
                        sender.sendMessage("Backup succesvol genaar Azure Blob Storage.");
                        getLogger().info("Backup succesvol genaar Azure Blob Storage.");
                    } else {
                        sender.sendMessage("Backup upload mislukt.");
                        getLogger().severe("Backup upload mislukt.");
                    }
                    Files.deleteIfExists(Paths.get(dumpPath, new String[0]));
                    Files.deleteIfExists(Paths.get(zipPath, new String[0]));
                    getLogger().info("Lokale bestanden verwijderd: " + dumpPath + ", " + zipPath);
                } catch (Exception e) {
                    e.printStackTrace();
                    sender.sendMessage("Er is een fout opgetreden tijdens het backup proces.");
                    getLogger().severe("Fout tijdens backup proces: " + e.getMessage());
                }
            });
            return true;
        }
        return false;
    }

    private String exportAllDatabases() throws IOException, InterruptedException {
        String timeStamp = (new SimpleDateFormat("yyyyMMdd_HHmmss")).format(new Date());
        String dumpFileName = "mysql_backup_" + timeStamp + ".sql";
        Path dumpFilePath = Paths.get(getDataFolder().getParentFile().getAbsolutePath(), new String[] { dumpFileName });
        ProcessBuilder pb = new ProcessBuilder(new String[] { "mysqldump", "-h", this.dbHost, "-P", this.dbPort, "-u", this.dbUsername, "-p" + this.dbPassword, "--all-databases" });
        pb.redirectOutput(dumpFilePath.toFile());
        pb.redirectErrorStream(true);
        Process process = pb.start();
        BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
        try {
            String line;
            while ((line = reader.readLine()) != null)
                getLogger().info(line);
            reader.close();
        } catch (Throwable throwable) {
            try {
                reader.close();
            } catch (Throwable throwable1) {
                throwable.addSuppressed(throwable1);
            }
            throw throwable;
        }
        int exitCode = process.waitFor();
        if (exitCode != 0)
            throw new IOException("mysqldump is mislukt met exit code: " + exitCode);
        return dumpFilePath.toString();
    }

    private String zipFile(String filePath) throws IOException {
        String timeStamp = (new SimpleDateFormat("yyyyMMdd_HHmmss")).format(new Date());
        String zipFileName = "backup_" + timeStamp + ".zip";
        Path zipFilePath = Paths.get(getDataFolder().getParentFile().getAbsolutePath(), new String[] { zipFileName });
        ZipOutputStream zos = new ZipOutputStream(new BufferedOutputStream(new FileOutputStream(zipFilePath.toFile())));
        try {
            File fileToZip = new File(filePath);
            FileInputStream fis = new FileInputStream(fileToZip);
            try {
                ZipEntry zipEntry = new ZipEntry(fileToZip.getName());
                zos.putNextEntry(zipEntry);
                byte[] buffer = new byte[4096];
                int len;
                while ((len = fis.read(buffer)) != -1)
                    zos.write(buffer, 0, len);
                zos.closeEntry();
                fis.close();
            } catch (Throwable throwable) {
                try {
                    fis.close();
                } catch (Throwable throwable1) {
                    throwable.addSuppressed(throwable1);
                }
                throw throwable;
            }
            zos.close();
        } catch (Throwable throwable) {
            try {
                zos.close();
            } catch (Throwable throwable1) {
                throwable.addSuppressed(throwable1);
            }
            throw throwable;
        }
        return zipFilePath.toString();
    }

    private boolean uploadToAzure(String zipPath) {
        File zipFile = new File(zipPath);
        if (!zipFile.exists()) {
            getLogger().severe("Het zipbestand bestaat niet: " + zipPath);
            return false;
        }
        try {
            BlobServiceClient blobServiceClient = (new BlobServiceClientBuilder()).connectionString(this.azureConnectionString).buildClient();
            BlobClient blobClient = blobServiceClient.getBlobContainerClient(this.azureContainerName).getBlobClient(zipFile.getName());
            blobClient.uploadFromFile(zipPath, true);
            getLogger().info("Bestand succesvol genaar Azure Blob Storage: " + blobClient.getBlobUrl());
            return true;
        } catch (Exception e) {
            getLogger().severe("Fout bij uploaden naar Azure Blob Storage: " + e.getMessage());
            e.printStackTrace();
            return false;
        }
    }
}
