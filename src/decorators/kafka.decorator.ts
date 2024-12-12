export const SUBSCRIBER_MAP = new Map();
export const SUBSCRIBER_OBJECT_MAP = new Map();
export const SCHEMAS = new Map();

export function Consume(topic: string) {
    return (target, propertyKey, descriptor) => {
        const originalMethod = target[propertyKey];
        SUBSCRIBER_MAP.set(topic, originalMethod);
        return descriptor;
    };
}
export function Subscribe(topic: string) {
    return (target, propertyKey, descriptor) => {
        const originalMethod = target[propertyKey];
        SUBSCRIBER_MAP.set(topic, originalMethod);
        return descriptor;
    };
}
