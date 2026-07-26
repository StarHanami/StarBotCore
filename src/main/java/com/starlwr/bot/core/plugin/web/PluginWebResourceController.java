package com.starlwr.bot.core.plugin.web;

import com.starlwr.bot.core.plugin.StarBotPlugin;
import com.starlwr.bot.core.plugin.StarBotPluginLoader;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.http.CacheControl;
import org.springframework.http.MediaType;
import org.springframework.http.MediaTypeFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;

import java.io.InputStream;
import java.time.Duration;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

@Controller
public class PluginWebResourceController {
    private final StarBotPluginLoader pluginLoader;

    public PluginWebResourceController(StarBotPluginLoader pluginLoader) {
        this.pluginLoader = pluginLoader;
    }

    @GetMapping({"/plugins/{alias}", "/plugins/{alias}/", "/plugins/{alias}/{*path}"})
    public ResponseEntity<ByteArrayResource> resource(@PathVariable String alias,
                                                       @PathVariable(required = false) String path) {
        StarBotPlugin plugin = pluginLoader.findPluginByWebAlias(alias).orElse(null);
        if (plugin == null) {
            return ResponseEntity.notFound().build();
        }
        String requested = path == null || path.isBlank() ? "index.html" : path.replaceFirst("^/", "");
        if (requested.equals("api") || requested.startsWith("api/") || requested.contains("..") || requested.startsWith("/")) {
            return ResponseEntity.notFound().build();
        }
        String entryName = plugin.getWebManifest().getWebRoot() + "/" + requested;
        try (JarFile jar = new JarFile(plugin.getJarFile())) {
            JarEntry entry = jar.getJarEntry(entryName);
            if (entry == null || entry.isDirectory()) {
                return ResponseEntity.notFound().build();
            }
            byte[] bytes;
            try (InputStream input = jar.getInputStream(entry)) {
                bytes = input.readAllBytes();
            }
            MediaType type = MediaTypeFactory.getMediaType(requested).orElse(MediaType.APPLICATION_OCTET_STREAM);
            CacheControl cache = requested.equals("index.html")
                    ? CacheControl.noCache()
                    : CacheControl.maxAge(Duration.ofHours(1)).cachePublic();
            return ResponseEntity.ok().contentType(type).cacheControl(cache).body(new ByteArrayResource(bytes));
        } catch (Exception e) {
            return ResponseEntity.internalServerError().build();
        }
    }
}
