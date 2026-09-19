package com.fix42.dashboard.connectors.transform;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The {@link RecordTransform} beans an application registered, by bean name.
 *
 * <p>A class rather than an injected {@code Map<String, RecordTransform>} so that the normal
 * case -- an application with no transforms at all -- is an empty registry rather than a
 * missing-bean failure at startup.
 *
 * <p>Resolution is by name and is deliberately strict: a connector naming a transform that is
 * not registered is a typo or a missing dependency, and silently running without the
 * enrichment would publish a table that looks fine and is wrong.
 */
public class TransformRegistry {

    private final Map<String, RecordTransform> transforms;

    public TransformRegistry(Map<String, RecordTransform> transforms) {
        this.transforms = new LinkedHashMap<>(transforms);
    }

    /** The registered names, for error messages. */
    public Set<String> names() {
        return transforms.keySet();
    }

    /** Whether a name is registered. Used by the validator without resolving anything. */
    public boolean contains(String name) {
        return transforms.containsKey(name);
    }

    /**
     * Resolve a connector's transform names, in the order it listed them.
     *
     * @param names the connector's {@code transforms:} list
     * @return the transforms to fold over each record
     * @throws IllegalStateException naming the first unknown transform and what is available
     */
    public List<RecordTransform> resolve(List<String> names) {
        List<RecordTransform> resolved = new ArrayList<>(names.size());
        for (String name : names) {
            RecordTransform transform = transforms.get(name);
            if (transform == null) {
                throw new IllegalStateException("no RecordTransform bean named '" + name
                        + "'; registered: " + transforms.keySet());
            }
            resolved.add(transform);
        }
        return List.copyOf(resolved);
    }
}
