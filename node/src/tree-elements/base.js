export function createElementHandler({ type, label, outputs = [], execute }) {
  if (!type) throw new Error("Element type is required");
  if (typeof execute !== "function") throw new Error(`Element ${type} missing execute()`);
  return {
    type,
    label: label ?? type,
    outputs,
    execute,
  };
}

export function getElementType(node) {
  return node?.data?.elementType ?? node?.nodeType ?? node?.type ?? null;
}

export function getNodeConfig(node) {
  return node?.data?.config ?? {};
}
