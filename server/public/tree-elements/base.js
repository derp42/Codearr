export function createElementDef({
  type,
  label,
  outputs = [],
  description = "",
  usage = "",
  fields = [],
  source = "built-in",
  plugin = null,
}) {
  return {
    type,
    label,
    outputs,
    description,
    usage,
    fields,
    source,
    plugin,
  };
}

export function normalizeElementNode(node, registry) {
  const rawType = node?.data?.elementType ?? node?.nodeType ?? node?.type ?? "custom";
  const elementType = rawType === "input_file" ? "input" : rawType;
  const def = registry.find((item) => item.type === elementType);
  return {
    ...node,
    type: "treeNode",
    data: {
      ...(node.data ?? {}),
      label: node?.data?.label ?? def?.label ?? elementType,
      elementType,
      outputs: node?.data?.outputs ?? def?.outputs ?? [{ id: "out", label: "out" }],
      __nodeId: node?.id ?? node?.data?.__nodeId,
    },
  };
}
