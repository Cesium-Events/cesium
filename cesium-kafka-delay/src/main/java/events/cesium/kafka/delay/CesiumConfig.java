package events.cesium.kafka.delay;

import java.nio.file.Paths;
import java.util.Collections;
import java.util.Map.Entry;
import java.util.Properties;

import org.cfg4j.source.ConfigurationSource;
import org.cfg4j.source.classpath.ClasspathConfigurationSource;
import org.cfg4j.source.context.environment.DefaultEnvironment;
import org.cfg4j.source.context.environment.ImmutableEnvironment;
import org.cfg4j.source.context.filesprovider.ConfigFilesProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CesiumConfig {

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final Properties props;

    public CesiumConfig(String environment) {
        // All configuration is in cesium-config.yaml
        ConfigFilesProvider configFilesProvider = () -> Collections.singletonList(Paths.get("cesium-config.yaml"));
        ConfigurationSource configSource = new ClasspathConfigurationSource(configFilesProvider);

        props = configSource.getConfiguration(new DefaultEnvironment());

        // If the runtime environment is specified as a property, also get that config
        // and merge them together
        if (environment == null || environment.trim().isEmpty()) {
            props.putAll(configSource.getConfiguration(new ImmutableEnvironment(environment)));
        }
    }

    public String getString(String key) {
        return getString(key, null);
    }

    public String getString(@SuppressWarnings("rawtypes") Class clazz, String propName) {
        return getString(clazz, propName, null);
    }

    public String getString(@SuppressWarnings("rawtypes") Class clazz, String propName, String defaultValue) {
        return getString(clazz.getName() + "." + propName, defaultValue);
    }

    public String getString(String key, String defaultValue) {
        Object value = props.get(key);
        if (value != null) {
            return String.valueOf(value);
        }
        log.debug("Returning default String config value [{}] for [{}]", defaultValue, key);
        return defaultValue;
    }

    public Integer getInteger(String key) {
        return getInteger(key, null);
    }

    public Integer getInteger(@SuppressWarnings("rawtypes") Class clazz, String propName) {
        return getInteger(clazz, propName, null);
    }

    public Integer getInteger(@SuppressWarnings("rawtypes") Class clazz, String propName, Integer defaultValue) {
        return getInteger(clazz.getName() + "." + propName, defaultValue);
    }

    public Integer getInteger(String key, Integer defaultValue) {
        Object value = props.get(key);
        if (value != null) {
            return Integer.parseInt(String.valueOf(value));
        }
        log.debug("Returning default Integer config value [{}] for [{}]", defaultValue, key);
        return defaultValue;
    }

    public Long getLong(String key) {
        return getLong(key, null);
    }

    public Long getLong(@SuppressWarnings("rawtypes") Class clazz, String propName) {
        return getLong(clazz, propName, null);
    }

    public Long getLong(@SuppressWarnings("rawtypes") Class clazz, String propName, Long defaultValue) {
        return getLong(clazz.getName() + "." + propName, defaultValue);
    }

    public Long getLong(String key, Long defaultValue) {
        Object value = props.get(key);
        if (value != null) {
            return Long.parseLong(String.valueOf(value));
        }
        log.debug("Returning default Long config value [{}] for [{}]", defaultValue, key);
        return defaultValue;
    }

    /**
     * Return a properties map of all the keys with the specified prefix. Trim the
     * prefix from the actual key name and put the suffix as the key in the returned
     * map.
     * 
     * <pre>
     * Ex. prefix is com.foo.Bar.MapValues and there is are property values
     *   com.foo.var.MapValues.val1->foo
     *   com.foo.var.MapValues.val2->bar
     * 
     * The returned properties map could contain the key->value pairs:
     *   val1->foo
     *   val2->bar
     * </pre>
     * 
     * @param clazz
     * @param propName
     * @return
     */
    public Properties getProperties(@SuppressWarnings("rawtypes") Class clazz, String propName) {
        Properties configProps = new Properties();

        String prefix = clazz.getName() + '.' + propName + '.';
        for (Entry<Object, Object> entry : props.entrySet()) {
            String prop = String.valueOf(entry.getKey());
            if (prop.startsWith(prefix)) {
                String key = prop.substring(prefix.length() + 1);
                configProps.put(key, String.valueOf(entry.getValue()));
            }
        }
        return configProps;
    }


    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (Entry<Object, Object> entry : props.entrySet()) {
            if (sb.length() > 0) {
                sb.append('\n');
            }
            sb.append(entry);
        }
        return sb.toString();
    }

    public static final void main(String[] args) {
        System.out.println(new CesiumConfig("home"));
    }
}