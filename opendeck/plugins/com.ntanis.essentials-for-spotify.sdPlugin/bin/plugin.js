import require$$0$3 from 'events';
import require$$1$1 from 'https';
import require$$2 from 'http';
import require$$3 from 'net';
import require$$4 from 'tls';
import require$$1 from 'crypto';
import require$$0$2 from 'stream';
import require$$7 from 'url';
import require$$0 from 'zlib';
import require$$0$1 from 'buffer';
import fs, { existsSync, readFileSync } from 'node:fs';
import path, { join } from 'node:path';
import { cwd } from 'node:process';
import crypto, { randomUUID } from 'node:crypto';
import EventEmitter$1, { EventEmitter as EventEmitter$2 } from 'node:events';
import http from 'node:http';
import { URL as URL$1 } from 'node:url';
import { exec, spawn } from 'child_process';
import os from 'os';
import https from 'node:https';

/**
 * Default language supported by all i18n providers.
 */
const defaultLanguage = "en";

/**
 * Creates a {@link IDisposable} that defers the disposing to the {@link dispose} function; disposing is guarded so that it may only occur once.
 * @param dispose Function responsible for disposing.
 * @returns Disposable whereby the disposing is delegated to the {@link dispose}  function.
 */
function deferredDisposable(dispose) {
    let isDisposed = false;
    const guardedDispose = () => {
        if (!isDisposed) {
            dispose();
            isDisposed = true;
        }
    };
    return {
        [Symbol.dispose]: guardedDispose,
        dispose: guardedDispose,
    };
}

/**
 * An event emitter that enables the listening for, and emitting of, events.
 */
class EventEmitter {
    /**
     * Underlying collection of events and their listeners.
     */
    events = new Map();
    /**
     * Adds the event {@link listener} for the event named {@link eventName}.
     * @param eventName Name of the event.
     * @param listener Event handler function.
     * @returns This instance with the {@link listener} added.
     */
    addListener(eventName, listener) {
        return this.add(eventName, listener, (listeners) => listeners.push({ listener }));
    }
    /**
     * Adds the event {@link listener} for the event named {@link eventName}, and returns a disposable capable of removing the event listener.
     * @param eventName Name of the event.
     * @param listener Event handler function.
     * @returns A disposable that removes the listener when disposed.
     */
    disposableOn(eventName, listener) {
        this.add(eventName, listener, (listeners) => listeners.push({ listener }));
        return deferredDisposable(() => this.removeListener(eventName, listener));
    }
    /**
     * Emits the {@link eventName}, invoking all event listeners with the specified {@link args}.
     * @param eventName Name of the event.
     * @param args Arguments supplied to each event listener.
     * @returns `true` when there was a listener associated with the event; otherwise `false`.
     */
    emit(eventName, ...args) {
        const listeners = this.events.get(eventName);
        if (listeners === undefined) {
            return false;
        }
        for (let i = 0; i < listeners.length;) {
            const { listener, once } = listeners[i];
            if (once) {
                this.remove(eventName, listeners, i);
            }
            else {
                i++;
            }
            listener(...args);
        }
        return true;
    }
    /**
     * Gets the event names with event listeners.
     * @returns Event names.
     */
    eventNames() {
        return Array.from(this.events.keys());
    }
    /**
     * Gets the number of event listeners for the event named {@link eventName}. When a {@link listener} is defined, only matching event listeners are counted.
     * @param eventName Name of the event.
     * @param listener Optional event listener to count.
     * @returns Number of event listeners.
     */
    listenerCount(eventName, listener) {
        const listeners = this.events.get(eventName);
        if (listeners === undefined || listener == undefined) {
            return listeners?.length || 0;
        }
        let count = 0;
        listeners.forEach((ev) => {
            if (ev.listener === listener) {
                count++;
            }
        });
        return count;
    }
    /**
     * Gets the event listeners for the event named {@link eventName}.
     * @param eventName Name of the event.
     * @returns The event listeners.
     */
    listeners(eventName) {
        return Array.from(this.events.get(eventName) || []).map(({ listener }) => listener);
    }
    /**
     * Removes the event {@link listener} for the event named {@link eventName}.
     * @param eventName Name of the event.
     * @param listener Event handler function.
     * @returns This instance with the event {@link listener} removed.
     */
    off(eventName, listener) {
        const listeners = this.events.get(eventName) ?? [];
        for (let i = listeners.length - 1; i >= 0; i--) {
            if (listeners[i].listener === listener) {
                this.remove(eventName, listeners, i);
            }
        }
        return this;
    }
    /**
     * Adds the event {@link listener} for the event named {@link eventName}.
     * @param eventName Name of the event.
     * @param listener Event handler function.
     * @returns This instance with the event {@link listener} added.
     */
    on(eventName, listener) {
        return this.add(eventName, listener, (listeners) => listeners.push({ listener }));
    }
    /**
     * Adds the **one-time** event {@link listener} for the event named {@link eventName}.
     * @param eventName Name of the event.
     * @param listener Event handler function.
     * @returns This instance with the event {@link listener} added.
     */
    once(eventName, listener) {
        return this.add(eventName, listener, (listeners) => listeners.push({ listener, once: true }));
    }
    /**
     * Adds the event {@link listener} to the beginning of the listeners for the event named {@link eventName}.
     * @param eventName Name of the event.
     * @param listener Event handler function.
     * @returns This instance with the event {@link listener} prepended.
     */
    prependListener(eventName, listener) {
        return this.add(eventName, listener, (listeners) => listeners.splice(0, 0, { listener }));
    }
    /**
     * Adds the **one-time** event {@link listener} to the beginning of the listeners for the event named {@link eventName}.
     * @param eventName Name of the event.
     * @param listener Event handler function.
     * @returns This instance with the event {@link listener} prepended.
     */
    prependOnceListener(eventName, listener) {
        return this.add(eventName, listener, (listeners) => listeners.splice(0, 0, { listener, once: true }));
    }
    /**
     * Removes all event listeners for the event named {@link eventName}.
     * @param eventName Name of the event.
     * @returns This instance with the event listeners removed
     */
    removeAllListeners(eventName) {
        const listeners = this.events.get(eventName) ?? [];
        while (listeners.length > 0) {
            this.remove(eventName, listeners, 0);
        }
        this.events.delete(eventName);
        return this;
    }
    /**
     * Removes the event {@link listener} for the event named {@link eventName}.
     * @param eventName Name of the event.
     * @param listener Event handler function.
     * @returns This instance with the event {@link listener} removed.
     */
    removeListener(eventName, listener) {
        return this.off(eventName, listener);
    }
    /**
     * Adds the event {@link listener} for the event named {@link eventName}.
     * @param eventName Name of the event.
     * @param listener Event handler function.
     * @param fn Function responsible for adding the new event handler function.
     * @returns This instance with event {@link listener} added.
     */
    add(eventName, listener, fn) {
        let listeners = this.events.get(eventName);
        if (listeners === undefined) {
            listeners = [];
            this.events.set(eventName, listeners);
        }
        fn(listeners);
        if (eventName !== "newListener") {
            const args = [eventName, listener];
            this.emit("newListener", ...args);
        }
        return this;
    }
    /**
     * Removes the listener at the given index.
     * @param eventName Name of the event.
     * @param listeners Listeners registered with the event.
     * @param index Index of the listener to remove.
     */
    remove(eventName, listeners, index) {
        const [{ listener }] = listeners.splice(index, 1);
        if (eventName !== "removeListener") {
            const args = [eventName, listener];
            this.emit("removeListener", ...args);
        }
    }
}

/**
 * Prevents the modification of existing property attributes and values on the value, and all of its child properties, and prevents the addition of new properties.
 * @param value Value to freeze.
 */
function freeze(value) {
    if (value !== undefined && value !== null && typeof value === "object" && !Object.isFrozen(value)) {
        Object.freeze(value);
        Object.values(value).forEach(freeze);
    }
}
/**
 * Gets the value at the specified {@link path}.
 * @param source Source object that is being read from.
 * @param path Path to the property to get.
 * @returns Value of the property.
 */
function get(source, path) {
    const props = path.split(".");
    return props.reduce((obj, prop) => obj && obj[prop], source);
}

/**
 * Internalization provider, responsible for managing localizations and translating resources.
 */
class I18nProvider {
    /**
     * Backing field for the default language.
     */
    #language;
    /**
     * Map of localized resources, indexed by their language.
     */
    #translations = new Map();
    /**
     * Function responsible for providing localized resources for a given language.
     */
    #readTranslations;
    /**
     * Internal events handler.
     */
    #events = new EventEmitter();
    /**
     * Initializes a new instance of the {@link I18nProvider} class.
     * @param language The default language to be used when retrieving translations for a given key.
     * @param readTranslations Function responsible for providing localized resources for a given language.
     */
    constructor(language, readTranslations) {
        this.#language = language;
        this.#readTranslations = readTranslations;
    }
    /**
     * The default language of the provider.
     * @returns The language.
     */
    get language() {
        return this.#language;
    }
    /**
     * The default language of the provider.
     * @param value The language.
     */
    set language(value) {
        if (this.#language !== value) {
            this.#language = value;
            this.#events.emit("languageChange", value);
        }
    }
    /**
     * Adds an event listener that is called when the language within the provider changes.
     * @param listener Listener function to be called.
     * @returns Resource manager that, when disposed, removes the event listener.
     */
    onLanguageChange(listener) {
        return this.#events.disposableOn("languageChange", listener);
    }
    /**
     * Translates the specified {@link key}, as defined within the resources for the {@link language}.
     * When the key is not found, the default language is checked. Alias of {@link I18nProvider.translate}.
     * @param key Key of the translation.
     * @param language Optional language to get the translation for; otherwise the default language.
     * @returns The translation; otherwise the key.
     */
    t(key, language = this.language) {
        return this.translate(key, language);
    }
    /**
     * Translates the specified {@link key}, as defined within the resources for the {@link language}.
     * When the key is not found, the default language is checked.
     * @param key Key of the translation.
     * @param language Optional language to get the translation for; otherwise the default language.
     * @returns The translation; otherwise the key.
     */
    translate(key, language = this.language) {
        // Determine the languages to search for.
        const languages = new Set([
            language,
            language.replaceAll("_", "-").split("-").at(0),
            defaultLanguage,
        ]);
        // Attempt to find the resource for the languages.
        for (const language of languages) {
            const resource = get(this.getTranslations(language), key);
            if (resource) {
                return resource.toString();
            }
        }
        // Otherwise fallback to the key.
        return key;
    }
    /**
     * Gets the translations for the specified language.
     * @param language Language whose translations are being retrieved.
     * @returns The translations; otherwise `null`.
     */
    getTranslations(language) {
        let translations = this.#translations.get(language);
        if (translations === undefined) {
            translations = this.#readTranslations(language);
            freeze(translations);
            this.#translations.set(language, translations);
        }
        return translations;
    }
}

/**
 * Provides a read-only iterable collection of items that also acts as a partial polyfill for iterator helpers.
 */
class Enumerable {
    /**
     * Backing function responsible for providing the iterator of items.
     */
    #items;
    /**
     * Backing function for {@link Enumerable.length}.
     */
    #length;
    /**
     * Captured iterator from the underlying iterable; used to fulfil {@link IterableIterator} methods.
     */
    #iterator;
    /**
     * Initializes a new instance of the {@link Enumerable} class.
     * @param source Source that contains the items.
     * @returns The enumerable.
     */
    constructor(source) {
        if (source instanceof Enumerable) {
            // Enumerable
            this.#items = source.#items;
            this.#length = source.#length;
        }
        else if (Array.isArray(source)) {
            // Array
            this.#items = () => source.values();
            this.#length = () => source.length;
        }
        else if (source instanceof Map || source instanceof Set) {
            // Map or Set
            this.#items = () => source.values();
            this.#length = () => source.size;
        }
        else {
            // IterableIterator delegate
            this.#items = source;
            this.#length = () => {
                let i = 0;
                // eslint-disable-next-line @typescript-eslint/no-unused-vars
                for (const _ of this) {
                    i++;
                }
                return i;
            };
        }
    }
    /**
     * Gets the number of items in the enumerable.
     * @returns The number of items.
     */
    get length() {
        return this.#length();
    }
    /**
     * Gets the iterator for the enumerable.
     * @yields The items.
     */
    *[Symbol.iterator]() {
        for (const item of this.#items()) {
            yield item;
        }
    }
    /**
     * Transforms each item within this iterator to an indexed pair, with each pair represented as an array.
     * @returns An iterator of indexed pairs.
     */
    asIndexedPairs() {
        return new Enumerable(function* () {
            let i = 0;
            for (const item of this) {
                yield [i++, item];
            }
        }.bind(this));
    }
    /**
     * Returns an iterator with the first items dropped, up to the specified limit.
     * @param limit The number of elements to drop from the start of the iteration.
     * @returns An iterator of items after the limit.
     */
    drop(limit) {
        if (isNaN(limit) || limit < 0) {
            throw new RangeError("limit must be 0, or a positive number");
        }
        return new Enumerable(function* () {
            let i = 0;
            for (const item of this) {
                if (i++ >= limit) {
                    yield item;
                }
            }
        }.bind(this));
    }
    /**
     * Determines whether all items satisfy the specified predicate.
     * @param predicate Function that determines whether each item fulfils the predicate.
     * @returns `true` when all items satisfy the predicate; otherwise `false`.
     */
    every(predicate) {
        for (const item of this) {
            if (!predicate(item)) {
                return false;
            }
        }
        return true;
    }
    /**
     * Returns an iterator of items that meet the specified predicate..
     * @param predicate Function that determines which items to filter.
     * @returns An iterator of filtered items.
     */
    filter(predicate) {
        return new Enumerable(function* () {
            for (const item of this) {
                if (predicate(item)) {
                    yield item;
                }
            }
        }.bind(this));
    }
    /**
     * Finds the first item that satisfies the specified predicate.
     * @param predicate Predicate to match items against.
     * @returns The first item that satisfied the predicate; otherwise `undefined`.
     */
    find(predicate) {
        for (const item of this) {
            if (predicate(item)) {
                return item;
            }
        }
    }
    /**
     * Finds the last item that satisfies the specified predicate.
     * @param predicate Predicate to match items against.
     * @returns The first item that satisfied the predicate; otherwise `undefined`.
     */
    findLast(predicate) {
        let result = undefined;
        for (const item of this) {
            if (predicate(item)) {
                result = item;
            }
        }
        return result;
    }
    /**
     * Returns an iterator containing items transformed using the specified mapper function.
     * @param mapper Function responsible for transforming each item.
     * @returns An iterator of transformed items.
     */
    flatMap(mapper) {
        return new Enumerable(function* () {
            for (const item of this) {
                for (const mapped of mapper(item)) {
                    yield mapped;
                }
            }
        }.bind(this));
    }
    /**
     * Iterates over each item, and invokes the specified function.
     * @param fn Function to invoke against each item.
     */
    forEach(fn) {
        for (const item of this) {
            fn(item);
        }
    }
    /**
     * Determines whether the search item exists in the collection exists.
     * @param search Item to search for.
     * @returns `true` when the item was found; otherwise `false`.
     */
    includes(search) {
        return this.some((item) => item === search);
    }
    /**
     * Returns an iterator of mapped items using the mapper function.
     * @param mapper Function responsible for mapping the items.
     * @returns An iterator of mapped items.
     */
    map(mapper) {
        return new Enumerable(function* () {
            for (const item of this) {
                yield mapper(item);
            }
        }.bind(this));
    }
    /**
     * Captures the underlying iterable, if it is not already captured, and gets the next item in the iterator.
     * @param args Optional values to send to the generator.
     * @returns An iterator result of the current iteration; when `done` is `false`, the current `value` is provided.
     */
    next(...args) {
        this.#iterator ??= this.#items();
        const result = this.#iterator.next(...args);
        if (result.done) {
            this.#iterator = undefined;
        }
        return result;
    }
    /**
     * Applies the accumulator function to each item, and returns the result.
     * @param accumulator Function responsible for accumulating all items within the collection.
     * @param initial Initial value supplied to the accumulator.
     * @returns Result of accumulating each value.
     */
    reduce(accumulator, initial) {
        if (this.length === 0) {
            if (initial === undefined) {
                throw new TypeError("Reduce of empty enumerable with no initial value.");
            }
            return initial;
        }
        let result = initial;
        for (const item of this) {
            if (result === undefined) {
                result = item;
            }
            else {
                result = accumulator(result, item);
            }
        }
        return result;
    }
    /**
     * Acts as if a `return` statement is inserted in the generator's body at the current suspended position.
     *
     * Please note, in the context of an {@link Enumerable}, calling {@link Enumerable.return} will clear the captured iterator,
     * if there is one. Subsequent calls to {@link Enumerable.next} will result in re-capturing the underlying iterable, and
     * yielding items from the beginning.
     * @param value Value to return.
     * @returns The value as an iterator result.
     */
    return(value) {
        this.#iterator = undefined;
        return { done: true, value };
    }
    /**
     * Determines whether an item in the collection exists that satisfies the specified predicate.
     * @param predicate Function used to search for an item.
     * @returns `true` when the item was found; otherwise `false`.
     */
    some(predicate) {
        for (const item of this) {
            if (predicate(item)) {
                return true;
            }
        }
        return false;
    }
    /**
     * Returns an iterator with the items, from 0, up to the specified limit.
     * @param limit Limit of items to take.
     * @returns An iterator of items from 0 to the limit.
     */
    take(limit) {
        if (isNaN(limit) || limit < 0) {
            throw new RangeError("limit must be 0, or a positive number");
        }
        return new Enumerable(function* () {
            let i = 0;
            for (const item of this) {
                if (i++ < limit) {
                    yield item;
                }
            }
        }.bind(this));
    }
    /**
     * Acts as if a `throw` statement is inserted in the generator's body at the current suspended position.
     * @param e Error to throw.
     */
    throw(e) {
        throw e;
    }
    /**
     * Converts this iterator to an array.
     * @returns The array of items from this iterator.
     */
    toArray() {
        return Array.from(this);
    }
    /**
     * Converts this iterator to serializable collection.
     * @returns The serializable collection of items.
     */
    toJSON() {
        return this.toArray();
    }
    /**
     * Converts this iterator to a string.
     * @returns The string.
     */
    toString() {
        return `${this.toArray()}`;
    }
}

// Polyfill, explicit resource management https://www.typescriptlang.org/docs/handbook/release-notes/typescript-5-2.html#using-declarations-and-explicit-resource-management
// eslint-disable-next-line @typescript-eslint/no-explicit-any
Symbol.dispose ??= Symbol("Symbol.dispose");

/**
 * Provides a wrapper around a value that is lazily instantiated.
 */
class Lazy {
    /**
     * Private backing field for {@link Lazy.value}.
     */
    #value = undefined;
    /**
     * Factory responsible for instantiating the value.
     */
    #valueFactory;
    /**
     * Initializes a new instance of the {@link Lazy} class.
     * @param valueFactory The factory responsible for instantiating the value.
     */
    constructor(valueFactory) {
        this.#valueFactory = valueFactory;
    }
    /**
     * Gets the value.
     * @returns The value.
     */
    get value() {
        if (this.#value === undefined) {
            this.#value = this.#valueFactory();
        }
        return this.#value;
    }
}

/**
 * Returns an object that contains a promise and two functions to resolve or reject it.
 * @returns The promise, and the resolve and reject functions.
 */
function withResolvers() {
    let resolve;
    let reject;
    const promise = new Promise((res, rej) => {
        resolve = res;
        reject = rej;
    });
    return { promise, resolve, reject };
}

function getDefaultExportFromCjs (x) {
	return x && x.__esModule && Object.prototype.hasOwnProperty.call(x, 'default') ? x['default'] : x;
}

var bufferUtil = {exports: {}};

var constants$1;
var hasRequiredConstants;

function requireConstants () {
	if (hasRequiredConstants) return constants$1;
	hasRequiredConstants = 1;

	const BINARY_TYPES = ['nodebuffer', 'arraybuffer', 'fragments'];
	const hasBlob = typeof Blob !== 'undefined';

	if (hasBlob) BINARY_TYPES.push('blob');

	constants$1 = {
	  BINARY_TYPES,
	  CLOSE_TIMEOUT: 30000,
	  EMPTY_BUFFER: Buffer.alloc(0),
	  GUID: '258EAFA5-E914-47DA-95CA-C5AB0DC85B11',
	  hasBlob,
	  kForOnEventAttribute: Symbol('kIsForOnEventAttribute'),
	  kListener: Symbol('kListener'),
	  kStatusCode: Symbol('status-code'),
	  kWebSocket: Symbol('websocket'),
	  NOOP: () => {}
	};
	return constants$1;
}

var hasRequiredBufferUtil;

function requireBufferUtil () {
	if (hasRequiredBufferUtil) return bufferUtil.exports;
	hasRequiredBufferUtil = 1;

	const { EMPTY_BUFFER } = requireConstants();

	const FastBuffer = Buffer[Symbol.species];

	/**
	 * Merges an array of buffers into a new buffer.
	 *
	 * @param {Buffer[]} list The array of buffers to concat
	 * @param {Number} totalLength The total length of buffers in the list
	 * @return {Buffer} The resulting buffer
	 * @public
	 */
	function concat(list, totalLength) {
	  if (list.length === 0) return EMPTY_BUFFER;
	  if (list.length === 1) return list[0];

	  const target = Buffer.allocUnsafe(totalLength);
	  let offset = 0;

	  for (let i = 0; i < list.length; i++) {
	    const buf = list[i];
	    target.set(buf, offset);
	    offset += buf.length;
	  }

	  if (offset < totalLength) {
	    return new FastBuffer(target.buffer, target.byteOffset, offset);
	  }

	  return target;
	}

	/**
	 * Masks a buffer using the given mask.
	 *
	 * @param {Buffer} source The buffer to mask
	 * @param {Buffer} mask The mask to use
	 * @param {Buffer} output The buffer where to store the result
	 * @param {Number} offset The offset at which to start writing
	 * @param {Number} length The number of bytes to mask.
	 * @public
	 */
	function _mask(source, mask, output, offset, length) {
	  for (let i = 0; i < length; i++) {
	    output[offset + i] = source[i] ^ mask[i & 3];
	  }
	}

	/**
	 * Unmasks a buffer using the given mask.
	 *
	 * @param {Buffer} buffer The buffer to unmask
	 * @param {Buffer} mask The mask to use
	 * @public
	 */
	function _unmask(buffer, mask) {
	  for (let i = 0; i < buffer.length; i++) {
	    buffer[i] ^= mask[i & 3];
	  }
	}

	/**
	 * Converts a buffer to an `ArrayBuffer`.
	 *
	 * @param {Buffer} buf The buffer to convert
	 * @return {ArrayBuffer} Converted buffer
	 * @public
	 */
	function toArrayBuffer(buf) {
	  if (buf.length === buf.buffer.byteLength) {
	    return buf.buffer;
	  }

	  return buf.buffer.slice(buf.byteOffset, buf.byteOffset + buf.length);
	}

	/**
	 * Converts `data` to a `Buffer`.
	 *
	 * @param {*} data The data to convert
	 * @return {Buffer} The buffer
	 * @throws {TypeError}
	 * @public
	 */
	function toBuffer(data) {
	  toBuffer.readOnly = true;

	  if (Buffer.isBuffer(data)) return data;

	  let buf;

	  if (data instanceof ArrayBuffer) {
	    buf = new FastBuffer(data);
	  } else if (ArrayBuffer.isView(data)) {
	    buf = new FastBuffer(data.buffer, data.byteOffset, data.byteLength);
	  } else {
	    buf = Buffer.from(data);
	    toBuffer.readOnly = false;
	  }

	  return buf;
	}

	bufferUtil.exports = {
	  concat,
	  mask: _mask,
	  toArrayBuffer,
	  toBuffer,
	  unmask: _unmask
	};

	/* istanbul ignore else  */
	if (!process.env.WS_NO_BUFFER_UTIL) {
	  try {
	    const bufferUtil$1 = require('bufferutil');

	    bufferUtil.exports.mask = function (source, mask, output, offset, length) {
	      if (length < 48) _mask(source, mask, output, offset, length);
	      else bufferUtil$1.mask(source, mask, output, offset, length);
	    };

	    bufferUtil.exports.unmask = function (buffer, mask) {
	      if (buffer.length < 32) _unmask(buffer, mask);
	      else bufferUtil$1.unmask(buffer, mask);
	    };
	  } catch (e) {
	    // Continue regardless of the error.
	  }
	}
	return bufferUtil.exports;
}

var limiter;
var hasRequiredLimiter;

function requireLimiter () {
	if (hasRequiredLimiter) return limiter;
	hasRequiredLimiter = 1;

	const kDone = Symbol('kDone');
	const kRun = Symbol('kRun');

	/**
	 * A very simple job queue with adjustable concurrency. Adapted from
	 * https://github.com/STRML/async-limiter
	 */
	class Limiter {
	  /**
	   * Creates a new `Limiter`.
	   *
	   * @param {Number} [concurrency=Infinity] The maximum number of jobs allowed
	   *     to run concurrently
	   */
	  constructor(concurrency) {
	    this[kDone] = () => {
	      this.pending--;
	      this[kRun]();
	    };
	    this.concurrency = concurrency || Infinity;
	    this.jobs = [];
	    this.pending = 0;
	  }

	  /**
	   * Adds a job to the queue.
	   *
	   * @param {Function} job The job to run
	   * @public
	   */
	  add(job) {
	    this.jobs.push(job);
	    this[kRun]();
	  }

	  /**
	   * Removes a job from the queue and runs it if possible.
	   *
	   * @private
	   */
	  [kRun]() {
	    if (this.pending === this.concurrency) return;

	    if (this.jobs.length) {
	      const job = this.jobs.shift();

	      this.pending++;
	      job(this[kDone]);
	    }
	  }
	}

	limiter = Limiter;
	return limiter;
}

var permessageDeflate;
var hasRequiredPermessageDeflate;

function requirePermessageDeflate () {
	if (hasRequiredPermessageDeflate) return permessageDeflate;
	hasRequiredPermessageDeflate = 1;

	const zlib = require$$0;

	const bufferUtil = requireBufferUtil();
	const Limiter = requireLimiter();
	const { kStatusCode } = requireConstants();

	const FastBuffer = Buffer[Symbol.species];
	const TRAILER = Buffer.from([0x00, 0x00, 0xff, 0xff]);
	const kPerMessageDeflate = Symbol('permessage-deflate');
	const kTotalLength = Symbol('total-length');
	const kCallback = Symbol('callback');
	const kBuffers = Symbol('buffers');
	const kError = Symbol('error');

	//
	// We limit zlib concurrency, which prevents severe memory fragmentation
	// as documented in https://github.com/nodejs/node/issues/8871#issuecomment-250915913
	// and https://github.com/websockets/ws/issues/1202
	//
	// Intentionally global; it's the global thread pool that's an issue.
	//
	let zlibLimiter;

	/**
	 * permessage-deflate implementation.
	 */
	class PerMessageDeflate {
	  /**
	   * Creates a PerMessageDeflate instance.
	   *
	   * @param {Object} [options] Configuration options
	   * @param {(Boolean|Number)} [options.clientMaxWindowBits] Advertise support
	   *     for, or request, a custom client window size
	   * @param {Boolean} [options.clientNoContextTakeover=false] Advertise/
	   *     acknowledge disabling of client context takeover
	   * @param {Number} [options.concurrencyLimit=10] The number of concurrent
	   *     calls to zlib
	   * @param {(Boolean|Number)} [options.serverMaxWindowBits] Request/confirm the
	   *     use of a custom server window size
	   * @param {Boolean} [options.serverNoContextTakeover=false] Request/accept
	   *     disabling of server context takeover
	   * @param {Number} [options.threshold=1024] Size (in bytes) below which
	   *     messages should not be compressed if context takeover is disabled
	   * @param {Object} [options.zlibDeflateOptions] Options to pass to zlib on
	   *     deflate
	   * @param {Object} [options.zlibInflateOptions] Options to pass to zlib on
	   *     inflate
	   * @param {Boolean} [isServer=false] Create the instance in either server or
	   *     client mode
	   * @param {Number} [maxPayload=0] The maximum allowed message length
	   */
	  constructor(options, isServer, maxPayload) {
	    this._maxPayload = maxPayload | 0;
	    this._options = options || {};
	    this._threshold =
	      this._options.threshold !== undefined ? this._options.threshold : 1024;
	    this._isServer = !!isServer;
	    this._deflate = null;
	    this._inflate = null;

	    this.params = null;

	    if (!zlibLimiter) {
	      const concurrency =
	        this._options.concurrencyLimit !== undefined
	          ? this._options.concurrencyLimit
	          : 10;
	      zlibLimiter = new Limiter(concurrency);
	    }
	  }

	  /**
	   * @type {String}
	   */
	  static get extensionName() {
	    return 'permessage-deflate';
	  }

	  /**
	   * Create an extension negotiation offer.
	   *
	   * @return {Object} Extension parameters
	   * @public
	   */
	  offer() {
	    const params = {};

	    if (this._options.serverNoContextTakeover) {
	      params.server_no_context_takeover = true;
	    }
	    if (this._options.clientNoContextTakeover) {
	      params.client_no_context_takeover = true;
	    }
	    if (this._options.serverMaxWindowBits) {
	      params.server_max_window_bits = this._options.serverMaxWindowBits;
	    }
	    if (this._options.clientMaxWindowBits) {
	      params.client_max_window_bits = this._options.clientMaxWindowBits;
	    } else if (this._options.clientMaxWindowBits == null) {
	      params.client_max_window_bits = true;
	    }

	    return params;
	  }

	  /**
	   * Accept an extension negotiation offer/response.
	   *
	   * @param {Array} configurations The extension negotiation offers/reponse
	   * @return {Object} Accepted configuration
	   * @public
	   */
	  accept(configurations) {
	    configurations = this.normalizeParams(configurations);

	    this.params = this._isServer
	      ? this.acceptAsServer(configurations)
	      : this.acceptAsClient(configurations);

	    return this.params;
	  }

	  /**
	   * Releases all resources used by the extension.
	   *
	   * @public
	   */
	  cleanup() {
	    if (this._inflate) {
	      this._inflate.close();
	      this._inflate = null;
	    }

	    if (this._deflate) {
	      const callback = this._deflate[kCallback];

	      this._deflate.close();
	      this._deflate = null;

	      if (callback) {
	        callback(
	          new Error(
	            'The deflate stream was closed while data was being processed'
	          )
	        );
	      }
	    }
	  }

	  /**
	   *  Accept an extension negotiation offer.
	   *
	   * @param {Array} offers The extension negotiation offers
	   * @return {Object} Accepted configuration
	   * @private
	   */
	  acceptAsServer(offers) {
	    const opts = this._options;
	    const accepted = offers.find((params) => {
	      if (
	        (opts.serverNoContextTakeover === false &&
	          params.server_no_context_takeover) ||
	        (params.server_max_window_bits &&
	          (opts.serverMaxWindowBits === false ||
	            (typeof opts.serverMaxWindowBits === 'number' &&
	              opts.serverMaxWindowBits > params.server_max_window_bits))) ||
	        (typeof opts.clientMaxWindowBits === 'number' &&
	          !params.client_max_window_bits)
	      ) {
	        return false;
	      }

	      return true;
	    });

	    if (!accepted) {
	      throw new Error('None of the extension offers can be accepted');
	    }

	    if (opts.serverNoContextTakeover) {
	      accepted.server_no_context_takeover = true;
	    }
	    if (opts.clientNoContextTakeover) {
	      accepted.client_no_context_takeover = true;
	    }
	    if (typeof opts.serverMaxWindowBits === 'number') {
	      accepted.server_max_window_bits = opts.serverMaxWindowBits;
	    }
	    if (typeof opts.clientMaxWindowBits === 'number') {
	      accepted.client_max_window_bits = opts.clientMaxWindowBits;
	    } else if (
	      accepted.client_max_window_bits === true ||
	      opts.clientMaxWindowBits === false
	    ) {
	      delete accepted.client_max_window_bits;
	    }

	    return accepted;
	  }

	  /**
	   * Accept the extension negotiation response.
	   *
	   * @param {Array} response The extension negotiation response
	   * @return {Object} Accepted configuration
	   * @private
	   */
	  acceptAsClient(response) {
	    const params = response[0];

	    if (
	      this._options.clientNoContextTakeover === false &&
	      params.client_no_context_takeover
	    ) {
	      throw new Error('Unexpected parameter "client_no_context_takeover"');
	    }

	    if (!params.client_max_window_bits) {
	      if (typeof this._options.clientMaxWindowBits === 'number') {
	        params.client_max_window_bits = this._options.clientMaxWindowBits;
	      }
	    } else if (
	      this._options.clientMaxWindowBits === false ||
	      (typeof this._options.clientMaxWindowBits === 'number' &&
	        params.client_max_window_bits > this._options.clientMaxWindowBits)
	    ) {
	      throw new Error(
	        'Unexpected or invalid parameter "client_max_window_bits"'
	      );
	    }

	    return params;
	  }

	  /**
	   * Normalize parameters.
	   *
	   * @param {Array} configurations The extension negotiation offers/reponse
	   * @return {Array} The offers/response with normalized parameters
	   * @private
	   */
	  normalizeParams(configurations) {
	    configurations.forEach((params) => {
	      Object.keys(params).forEach((key) => {
	        let value = params[key];

	        if (value.length > 1) {
	          throw new Error(`Parameter "${key}" must have only a single value`);
	        }

	        value = value[0];

	        if (key === 'client_max_window_bits') {
	          if (value !== true) {
	            const num = +value;
	            if (!Number.isInteger(num) || num < 8 || num > 15) {
	              throw new TypeError(
	                `Invalid value for parameter "${key}": ${value}`
	              );
	            }
	            value = num;
	          } else if (!this._isServer) {
	            throw new TypeError(
	              `Invalid value for parameter "${key}": ${value}`
	            );
	          }
	        } else if (key === 'server_max_window_bits') {
	          const num = +value;
	          if (!Number.isInteger(num) || num < 8 || num > 15) {
	            throw new TypeError(
	              `Invalid value for parameter "${key}": ${value}`
	            );
	          }
	          value = num;
	        } else if (
	          key === 'client_no_context_takeover' ||
	          key === 'server_no_context_takeover'
	        ) {
	          if (value !== true) {
	            throw new TypeError(
	              `Invalid value for parameter "${key}": ${value}`
	            );
	          }
	        } else {
	          throw new Error(`Unknown parameter "${key}"`);
	        }

	        params[key] = value;
	      });
	    });

	    return configurations;
	  }

	  /**
	   * Decompress data. Concurrency limited.
	   *
	   * @param {Buffer} data Compressed data
	   * @param {Boolean} fin Specifies whether or not this is the last fragment
	   * @param {Function} callback Callback
	   * @public
	   */
	  decompress(data, fin, callback) {
	    zlibLimiter.add((done) => {
	      this._decompress(data, fin, (err, result) => {
	        done();
	        callback(err, result);
	      });
	    });
	  }

	  /**
	   * Compress data. Concurrency limited.
	   *
	   * @param {(Buffer|String)} data Data to compress
	   * @param {Boolean} fin Specifies whether or not this is the last fragment
	   * @param {Function} callback Callback
	   * @public
	   */
	  compress(data, fin, callback) {
	    zlibLimiter.add((done) => {
	      this._compress(data, fin, (err, result) => {
	        done();
	        callback(err, result);
	      });
	    });
	  }

	  /**
	   * Decompress data.
	   *
	   * @param {Buffer} data Compressed data
	   * @param {Boolean} fin Specifies whether or not this is the last fragment
	   * @param {Function} callback Callback
	   * @private
	   */
	  _decompress(data, fin, callback) {
	    const endpoint = this._isServer ? 'client' : 'server';

	    if (!this._inflate) {
	      const key = `${endpoint}_max_window_bits`;
	      const windowBits =
	        typeof this.params[key] !== 'number'
	          ? zlib.Z_DEFAULT_WINDOWBITS
	          : this.params[key];

	      this._inflate = zlib.createInflateRaw({
	        ...this._options.zlibInflateOptions,
	        windowBits
	      });
	      this._inflate[kPerMessageDeflate] = this;
	      this._inflate[kTotalLength] = 0;
	      this._inflate[kBuffers] = [];
	      this._inflate.on('error', inflateOnError);
	      this._inflate.on('data', inflateOnData);
	    }

	    this._inflate[kCallback] = callback;

	    this._inflate.write(data);
	    if (fin) this._inflate.write(TRAILER);

	    this._inflate.flush(() => {
	      const err = this._inflate[kError];

	      if (err) {
	        this._inflate.close();
	        this._inflate = null;
	        callback(err);
	        return;
	      }

	      const data = bufferUtil.concat(
	        this._inflate[kBuffers],
	        this._inflate[kTotalLength]
	      );

	      if (this._inflate._readableState.endEmitted) {
	        this._inflate.close();
	        this._inflate = null;
	      } else {
	        this._inflate[kTotalLength] = 0;
	        this._inflate[kBuffers] = [];

	        if (fin && this.params[`${endpoint}_no_context_takeover`]) {
	          this._inflate.reset();
	        }
	      }

	      callback(null, data);
	    });
	  }

	  /**
	   * Compress data.
	   *
	   * @param {(Buffer|String)} data Data to compress
	   * @param {Boolean} fin Specifies whether or not this is the last fragment
	   * @param {Function} callback Callback
	   * @private
	   */
	  _compress(data, fin, callback) {
	    const endpoint = this._isServer ? 'server' : 'client';

	    if (!this._deflate) {
	      const key = `${endpoint}_max_window_bits`;
	      const windowBits =
	        typeof this.params[key] !== 'number'
	          ? zlib.Z_DEFAULT_WINDOWBITS
	          : this.params[key];

	      this._deflate = zlib.createDeflateRaw({
	        ...this._options.zlibDeflateOptions,
	        windowBits
	      });

	      this._deflate[kTotalLength] = 0;
	      this._deflate[kBuffers] = [];

	      this._deflate.on('data', deflateOnData);
	    }

	    this._deflate[kCallback] = callback;

	    this._deflate.write(data);
	    this._deflate.flush(zlib.Z_SYNC_FLUSH, () => {
	      if (!this._deflate) {
	        //
	        // The deflate stream was closed while data was being processed.
	        //
	        return;
	      }

	      let data = bufferUtil.concat(
	        this._deflate[kBuffers],
	        this._deflate[kTotalLength]
	      );

	      if (fin) {
	        data = new FastBuffer(data.buffer, data.byteOffset, data.length - 4);
	      }

	      //
	      // Ensure that the callback will not be called again in
	      // `PerMessageDeflate#cleanup()`.
	      //
	      this._deflate[kCallback] = null;

	      this._deflate[kTotalLength] = 0;
	      this._deflate[kBuffers] = [];

	      if (fin && this.params[`${endpoint}_no_context_takeover`]) {
	        this._deflate.reset();
	      }

	      callback(null, data);
	    });
	  }
	}

	permessageDeflate = PerMessageDeflate;

	/**
	 * The listener of the `zlib.DeflateRaw` stream `'data'` event.
	 *
	 * @param {Buffer} chunk A chunk of data
	 * @private
	 */
	function deflateOnData(chunk) {
	  this[kBuffers].push(chunk);
	  this[kTotalLength] += chunk.length;
	}

	/**
	 * The listener of the `zlib.InflateRaw` stream `'data'` event.
	 *
	 * @param {Buffer} chunk A chunk of data
	 * @private
	 */
	function inflateOnData(chunk) {
	  this[kTotalLength] += chunk.length;

	  if (
	    this[kPerMessageDeflate]._maxPayload < 1 ||
	    this[kTotalLength] <= this[kPerMessageDeflate]._maxPayload
	  ) {
	    this[kBuffers].push(chunk);
	    return;
	  }

	  this[kError] = new RangeError('Max payload size exceeded');
	  this[kError].code = 'WS_ERR_UNSUPPORTED_MESSAGE_LENGTH';
	  this[kError][kStatusCode] = 1009;
	  this.removeListener('data', inflateOnData);

	  //
	  // The choice to employ `zlib.reset()` over `zlib.close()` is dictated by the
	  // fact that in Node.js versions prior to 13.10.0, the callback for
	  // `zlib.flush()` is not called if `zlib.close()` is used. Utilizing
	  // `zlib.reset()` ensures that either the callback is invoked or an error is
	  // emitted.
	  //
	  this.reset();
	}

	/**
	 * The listener of the `zlib.InflateRaw` stream `'error'` event.
	 *
	 * @param {Error} err The emitted error
	 * @private
	 */
	function inflateOnError(err) {
	  //
	  // There is no need to call `Zlib#close()` as the handle is automatically
	  // closed when an error is emitted.
	  //
	  this[kPerMessageDeflate]._inflate = null;

	  if (this[kError]) {
	    this[kCallback](this[kError]);
	    return;
	  }

	  err[kStatusCode] = 1007;
	  this[kCallback](err);
	}
	return permessageDeflate;
}

var validation = {exports: {}};

var hasRequiredValidation;

function requireValidation () {
	if (hasRequiredValidation) return validation.exports;
	hasRequiredValidation = 1;

	const { isUtf8 } = require$$0$1;

	const { hasBlob } = requireConstants();

	//
	// Allowed token characters:
	//
	// '!', '#', '$', '%', '&', ''', '*', '+', '-',
	// '.', 0-9, A-Z, '^', '_', '`', a-z, '|', '~'
	//
	// tokenChars[32] === 0 // ' '
	// tokenChars[33] === 1 // '!'
	// tokenChars[34] === 0 // '"'
	// ...
	//
	// prettier-ignore
	const tokenChars = [
	  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // 0 - 15
	  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // 16 - 31
	  0, 1, 0, 1, 1, 1, 1, 1, 0, 0, 1, 1, 0, 1, 1, 0, // 32 - 47
	  1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0, // 48 - 63
	  0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, // 64 - 79
	  1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 0, 0, 1, 1, // 80 - 95
	  1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, // 96 - 111
	  1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 1, 0, 1, 0 // 112 - 127
	];

	/**
	 * Checks if a status code is allowed in a close frame.
	 *
	 * @param {Number} code The status code
	 * @return {Boolean} `true` if the status code is valid, else `false`
	 * @public
	 */
	function isValidStatusCode(code) {
	  return (
	    (code >= 1000 &&
	      code <= 1014 &&
	      code !== 1004 &&
	      code !== 1005 &&
	      code !== 1006) ||
	    (code >= 3000 && code <= 4999)
	  );
	}

	/**
	 * Checks if a given buffer contains only correct UTF-8.
	 * Ported from https://www.cl.cam.ac.uk/%7Emgk25/ucs/utf8_check.c by
	 * Markus Kuhn.
	 *
	 * @param {Buffer} buf The buffer to check
	 * @return {Boolean} `true` if `buf` contains only correct UTF-8, else `false`
	 * @public
	 */
	function _isValidUTF8(buf) {
	  const len = buf.length;
	  let i = 0;

	  while (i < len) {
	    if ((buf[i] & 0x80) === 0) {
	      // 0xxxxxxx
	      i++;
	    } else if ((buf[i] & 0xe0) === 0xc0) {
	      // 110xxxxx 10xxxxxx
	      if (
	        i + 1 === len ||
	        (buf[i + 1] & 0xc0) !== 0x80 ||
	        (buf[i] & 0xfe) === 0xc0 // Overlong
	      ) {
	        return false;
	      }

	      i += 2;
	    } else if ((buf[i] & 0xf0) === 0xe0) {
	      // 1110xxxx 10xxxxxx 10xxxxxx
	      if (
	        i + 2 >= len ||
	        (buf[i + 1] & 0xc0) !== 0x80 ||
	        (buf[i + 2] & 0xc0) !== 0x80 ||
	        (buf[i] === 0xe0 && (buf[i + 1] & 0xe0) === 0x80) || // Overlong
	        (buf[i] === 0xed && (buf[i + 1] & 0xe0) === 0xa0) // Surrogate (U+D800 - U+DFFF)
	      ) {
	        return false;
	      }

	      i += 3;
	    } else if ((buf[i] & 0xf8) === 0xf0) {
	      // 11110xxx 10xxxxxx 10xxxxxx 10xxxxxx
	      if (
	        i + 3 >= len ||
	        (buf[i + 1] & 0xc0) !== 0x80 ||
	        (buf[i + 2] & 0xc0) !== 0x80 ||
	        (buf[i + 3] & 0xc0) !== 0x80 ||
	        (buf[i] === 0xf0 && (buf[i + 1] & 0xf0) === 0x80) || // Overlong
	        (buf[i] === 0xf4 && buf[i + 1] > 0x8f) ||
	        buf[i] > 0xf4 // > U+10FFFF
	      ) {
	        return false;
	      }

	      i += 4;
	    } else {
	      return false;
	    }
	  }

	  return true;
	}

	/**
	 * Determines whether a value is a `Blob`.
	 *
	 * @param {*} value The value to be tested
	 * @return {Boolean} `true` if `value` is a `Blob`, else `false`
	 * @private
	 */
	function isBlob(value) {
	  return (
	    hasBlob &&
	    typeof value === 'object' &&
	    typeof value.arrayBuffer === 'function' &&
	    typeof value.type === 'string' &&
	    typeof value.stream === 'function' &&
	    (value[Symbol.toStringTag] === 'Blob' ||
	      value[Symbol.toStringTag] === 'File')
	  );
	}

	validation.exports = {
	  isBlob,
	  isValidStatusCode,
	  isValidUTF8: _isValidUTF8,
	  tokenChars
	};

	if (isUtf8) {
	  validation.exports.isValidUTF8 = function (buf) {
	    return buf.length < 24 ? _isValidUTF8(buf) : isUtf8(buf);
	  };
	} /* istanbul ignore else  */ else if (!process.env.WS_NO_UTF_8_VALIDATE) {
	  try {
	    const isValidUTF8 = require('utf-8-validate');

	    validation.exports.isValidUTF8 = function (buf) {
	      return buf.length < 32 ? _isValidUTF8(buf) : isValidUTF8(buf);
	    };
	  } catch (e) {
	    // Continue regardless of the error.
	  }
	}
	return validation.exports;
}

var receiver;
var hasRequiredReceiver;

function requireReceiver () {
	if (hasRequiredReceiver) return receiver;
	hasRequiredReceiver = 1;

	const { Writable } = require$$0$2;

	const PerMessageDeflate = requirePermessageDeflate();
	const {
	  BINARY_TYPES,
	  EMPTY_BUFFER,
	  kStatusCode,
	  kWebSocket
	} = requireConstants();
	const { concat, toArrayBuffer, unmask } = requireBufferUtil();
	const { isValidStatusCode, isValidUTF8 } = requireValidation();

	const FastBuffer = Buffer[Symbol.species];

	const GET_INFO = 0;
	const GET_PAYLOAD_LENGTH_16 = 1;
	const GET_PAYLOAD_LENGTH_64 = 2;
	const GET_MASK = 3;
	const GET_DATA = 4;
	const INFLATING = 5;
	const DEFER_EVENT = 6;

	/**
	 * HyBi Receiver implementation.
	 *
	 * @extends Writable
	 */
	class Receiver extends Writable {
	  /**
	   * Creates a Receiver instance.
	   *
	   * @param {Object} [options] Options object
	   * @param {Boolean} [options.allowSynchronousEvents=true] Specifies whether
	   *     any of the `'message'`, `'ping'`, and `'pong'` events can be emitted
	   *     multiple times in the same tick
	   * @param {String} [options.binaryType=nodebuffer] The type for binary data
	   * @param {Object} [options.extensions] An object containing the negotiated
	   *     extensions
	   * @param {Boolean} [options.isServer=false] Specifies whether to operate in
	   *     client or server mode
	   * @param {Number} [options.maxPayload=0] The maximum allowed message length
	   * @param {Boolean} [options.skipUTF8Validation=false] Specifies whether or
	   *     not to skip UTF-8 validation for text and close messages
	   */
	  constructor(options = {}) {
	    super();

	    this._allowSynchronousEvents =
	      options.allowSynchronousEvents !== undefined
	        ? options.allowSynchronousEvents
	        : true;
	    this._binaryType = options.binaryType || BINARY_TYPES[0];
	    this._extensions = options.extensions || {};
	    this._isServer = !!options.isServer;
	    this._maxPayload = options.maxPayload | 0;
	    this._skipUTF8Validation = !!options.skipUTF8Validation;
	    this[kWebSocket] = undefined;

	    this._bufferedBytes = 0;
	    this._buffers = [];

	    this._compressed = false;
	    this._payloadLength = 0;
	    this._mask = undefined;
	    this._fragmented = 0;
	    this._masked = false;
	    this._fin = false;
	    this._opcode = 0;

	    this._totalPayloadLength = 0;
	    this._messageLength = 0;
	    this._fragments = [];

	    this._errored = false;
	    this._loop = false;
	    this._state = GET_INFO;
	  }

	  /**
	   * Implements `Writable.prototype._write()`.
	   *
	   * @param {Buffer} chunk The chunk of data to write
	   * @param {String} encoding The character encoding of `chunk`
	   * @param {Function} cb Callback
	   * @private
	   */
	  _write(chunk, encoding, cb) {
	    if (this._opcode === 0x08 && this._state == GET_INFO) return cb();

	    this._bufferedBytes += chunk.length;
	    this._buffers.push(chunk);
	    this.startLoop(cb);
	  }

	  /**
	   * Consumes `n` bytes from the buffered data.
	   *
	   * @param {Number} n The number of bytes to consume
	   * @return {Buffer} The consumed bytes
	   * @private
	   */
	  consume(n) {
	    this._bufferedBytes -= n;

	    if (n === this._buffers[0].length) return this._buffers.shift();

	    if (n < this._buffers[0].length) {
	      const buf = this._buffers[0];
	      this._buffers[0] = new FastBuffer(
	        buf.buffer,
	        buf.byteOffset + n,
	        buf.length - n
	      );

	      return new FastBuffer(buf.buffer, buf.byteOffset, n);
	    }

	    const dst = Buffer.allocUnsafe(n);

	    do {
	      const buf = this._buffers[0];
	      const offset = dst.length - n;

	      if (n >= buf.length) {
	        dst.set(this._buffers.shift(), offset);
	      } else {
	        dst.set(new Uint8Array(buf.buffer, buf.byteOffset, n), offset);
	        this._buffers[0] = new FastBuffer(
	          buf.buffer,
	          buf.byteOffset + n,
	          buf.length - n
	        );
	      }

	      n -= buf.length;
	    } while (n > 0);

	    return dst;
	  }

	  /**
	   * Starts the parsing loop.
	   *
	   * @param {Function} cb Callback
	   * @private
	   */
	  startLoop(cb) {
	    this._loop = true;

	    do {
	      switch (this._state) {
	        case GET_INFO:
	          this.getInfo(cb);
	          break;
	        case GET_PAYLOAD_LENGTH_16:
	          this.getPayloadLength16(cb);
	          break;
	        case GET_PAYLOAD_LENGTH_64:
	          this.getPayloadLength64(cb);
	          break;
	        case GET_MASK:
	          this.getMask();
	          break;
	        case GET_DATA:
	          this.getData(cb);
	          break;
	        case INFLATING:
	        case DEFER_EVENT:
	          this._loop = false;
	          return;
	      }
	    } while (this._loop);

	    if (!this._errored) cb();
	  }

	  /**
	   * Reads the first two bytes of a frame.
	   *
	   * @param {Function} cb Callback
	   * @private
	   */
	  getInfo(cb) {
	    if (this._bufferedBytes < 2) {
	      this._loop = false;
	      return;
	    }

	    const buf = this.consume(2);

	    if ((buf[0] & 0x30) !== 0x00) {
	      const error = this.createError(
	        RangeError,
	        'RSV2 and RSV3 must be clear',
	        true,
	        1002,
	        'WS_ERR_UNEXPECTED_RSV_2_3'
	      );

	      cb(error);
	      return;
	    }

	    const compressed = (buf[0] & 0x40) === 0x40;

	    if (compressed && !this._extensions[PerMessageDeflate.extensionName]) {
	      const error = this.createError(
	        RangeError,
	        'RSV1 must be clear',
	        true,
	        1002,
	        'WS_ERR_UNEXPECTED_RSV_1'
	      );

	      cb(error);
	      return;
	    }

	    this._fin = (buf[0] & 0x80) === 0x80;
	    this._opcode = buf[0] & 0x0f;
	    this._payloadLength = buf[1] & 0x7f;

	    if (this._opcode === 0x00) {
	      if (compressed) {
	        const error = this.createError(
	          RangeError,
	          'RSV1 must be clear',
	          true,
	          1002,
	          'WS_ERR_UNEXPECTED_RSV_1'
	        );

	        cb(error);
	        return;
	      }

	      if (!this._fragmented) {
	        const error = this.createError(
	          RangeError,
	          'invalid opcode 0',
	          true,
	          1002,
	          'WS_ERR_INVALID_OPCODE'
	        );

	        cb(error);
	        return;
	      }

	      this._opcode = this._fragmented;
	    } else if (this._opcode === 0x01 || this._opcode === 0x02) {
	      if (this._fragmented) {
	        const error = this.createError(
	          RangeError,
	          `invalid opcode ${this._opcode}`,
	          true,
	          1002,
	          'WS_ERR_INVALID_OPCODE'
	        );

	        cb(error);
	        return;
	      }

	      this._compressed = compressed;
	    } else if (this._opcode > 0x07 && this._opcode < 0x0b) {
	      if (!this._fin) {
	        const error = this.createError(
	          RangeError,
	          'FIN must be set',
	          true,
	          1002,
	          'WS_ERR_EXPECTED_FIN'
	        );

	        cb(error);
	        return;
	      }

	      if (compressed) {
	        const error = this.createError(
	          RangeError,
	          'RSV1 must be clear',
	          true,
	          1002,
	          'WS_ERR_UNEXPECTED_RSV_1'
	        );

	        cb(error);
	        return;
	      }

	      if (
	        this._payloadLength > 0x7d ||
	        (this._opcode === 0x08 && this._payloadLength === 1)
	      ) {
	        const error = this.createError(
	          RangeError,
	          `invalid payload length ${this._payloadLength}`,
	          true,
	          1002,
	          'WS_ERR_INVALID_CONTROL_PAYLOAD_LENGTH'
	        );

	        cb(error);
	        return;
	      }
	    } else {
	      const error = this.createError(
	        RangeError,
	        `invalid opcode ${this._opcode}`,
	        true,
	        1002,
	        'WS_ERR_INVALID_OPCODE'
	      );

	      cb(error);
	      return;
	    }

	    if (!this._fin && !this._fragmented) this._fragmented = this._opcode;
	    this._masked = (buf[1] & 0x80) === 0x80;

	    if (this._isServer) {
	      if (!this._masked) {
	        const error = this.createError(
	          RangeError,
	          'MASK must be set',
	          true,
	          1002,
	          'WS_ERR_EXPECTED_MASK'
	        );

	        cb(error);
	        return;
	      }
	    } else if (this._masked) {
	      const error = this.createError(
	        RangeError,
	        'MASK must be clear',
	        true,
	        1002,
	        'WS_ERR_UNEXPECTED_MASK'
	      );

	      cb(error);
	      return;
	    }

	    if (this._payloadLength === 126) this._state = GET_PAYLOAD_LENGTH_16;
	    else if (this._payloadLength === 127) this._state = GET_PAYLOAD_LENGTH_64;
	    else this.haveLength(cb);
	  }

	  /**
	   * Gets extended payload length (7+16).
	   *
	   * @param {Function} cb Callback
	   * @private
	   */
	  getPayloadLength16(cb) {
	    if (this._bufferedBytes < 2) {
	      this._loop = false;
	      return;
	    }

	    this._payloadLength = this.consume(2).readUInt16BE(0);
	    this.haveLength(cb);
	  }

	  /**
	   * Gets extended payload length (7+64).
	   *
	   * @param {Function} cb Callback
	   * @private
	   */
	  getPayloadLength64(cb) {
	    if (this._bufferedBytes < 8) {
	      this._loop = false;
	      return;
	    }

	    const buf = this.consume(8);
	    const num = buf.readUInt32BE(0);

	    //
	    // The maximum safe integer in JavaScript is 2^53 - 1. An error is returned
	    // if payload length is greater than this number.
	    //
	    if (num > Math.pow(2, 53 - 32) - 1) {
	      const error = this.createError(
	        RangeError,
	        'Unsupported WebSocket frame: payload length > 2^53 - 1',
	        false,
	        1009,
	        'WS_ERR_UNSUPPORTED_DATA_PAYLOAD_LENGTH'
	      );

	      cb(error);
	      return;
	    }

	    this._payloadLength = num * Math.pow(2, 32) + buf.readUInt32BE(4);
	    this.haveLength(cb);
	  }

	  /**
	   * Payload length has been read.
	   *
	   * @param {Function} cb Callback
	   * @private
	   */
	  haveLength(cb) {
	    if (this._payloadLength && this._opcode < 0x08) {
	      this._totalPayloadLength += this._payloadLength;
	      if (this._totalPayloadLength > this._maxPayload && this._maxPayload > 0) {
	        const error = this.createError(
	          RangeError,
	          'Max payload size exceeded',
	          false,
	          1009,
	          'WS_ERR_UNSUPPORTED_MESSAGE_LENGTH'
	        );

	        cb(error);
	        return;
	      }
	    }

	    if (this._masked) this._state = GET_MASK;
	    else this._state = GET_DATA;
	  }

	  /**
	   * Reads mask bytes.
	   *
	   * @private
	   */
	  getMask() {
	    if (this._bufferedBytes < 4) {
	      this._loop = false;
	      return;
	    }

	    this._mask = this.consume(4);
	    this._state = GET_DATA;
	  }

	  /**
	   * Reads data bytes.
	   *
	   * @param {Function} cb Callback
	   * @private
	   */
	  getData(cb) {
	    let data = EMPTY_BUFFER;

	    if (this._payloadLength) {
	      if (this._bufferedBytes < this._payloadLength) {
	        this._loop = false;
	        return;
	      }

	      data = this.consume(this._payloadLength);

	      if (
	        this._masked &&
	        (this._mask[0] | this._mask[1] | this._mask[2] | this._mask[3]) !== 0
	      ) {
	        unmask(data, this._mask);
	      }
	    }

	    if (this._opcode > 0x07) {
	      this.controlMessage(data, cb);
	      return;
	    }

	    if (this._compressed) {
	      this._state = INFLATING;
	      this.decompress(data, cb);
	      return;
	    }

	    if (data.length) {
	      //
	      // This message is not compressed so its length is the sum of the payload
	      // length of all fragments.
	      //
	      this._messageLength = this._totalPayloadLength;
	      this._fragments.push(data);
	    }

	    this.dataMessage(cb);
	  }

	  /**
	   * Decompresses data.
	   *
	   * @param {Buffer} data Compressed data
	   * @param {Function} cb Callback
	   * @private
	   */
	  decompress(data, cb) {
	    const perMessageDeflate = this._extensions[PerMessageDeflate.extensionName];

	    perMessageDeflate.decompress(data, this._fin, (err, buf) => {
	      if (err) return cb(err);

	      if (buf.length) {
	        this._messageLength += buf.length;
	        if (this._messageLength > this._maxPayload && this._maxPayload > 0) {
	          const error = this.createError(
	            RangeError,
	            'Max payload size exceeded',
	            false,
	            1009,
	            'WS_ERR_UNSUPPORTED_MESSAGE_LENGTH'
	          );

	          cb(error);
	          return;
	        }

	        this._fragments.push(buf);
	      }

	      this.dataMessage(cb);
	      if (this._state === GET_INFO) this.startLoop(cb);
	    });
	  }

	  /**
	   * Handles a data message.
	   *
	   * @param {Function} cb Callback
	   * @private
	   */
	  dataMessage(cb) {
	    if (!this._fin) {
	      this._state = GET_INFO;
	      return;
	    }

	    const messageLength = this._messageLength;
	    const fragments = this._fragments;

	    this._totalPayloadLength = 0;
	    this._messageLength = 0;
	    this._fragmented = 0;
	    this._fragments = [];

	    if (this._opcode === 2) {
	      let data;

	      if (this._binaryType === 'nodebuffer') {
	        data = concat(fragments, messageLength);
	      } else if (this._binaryType === 'arraybuffer') {
	        data = toArrayBuffer(concat(fragments, messageLength));
	      } else if (this._binaryType === 'blob') {
	        data = new Blob(fragments);
	      } else {
	        data = fragments;
	      }

	      if (this._allowSynchronousEvents) {
	        this.emit('message', data, true);
	        this._state = GET_INFO;
	      } else {
	        this._state = DEFER_EVENT;
	        setImmediate(() => {
	          this.emit('message', data, true);
	          this._state = GET_INFO;
	          this.startLoop(cb);
	        });
	      }
	    } else {
	      const buf = concat(fragments, messageLength);

	      if (!this._skipUTF8Validation && !isValidUTF8(buf)) {
	        const error = this.createError(
	          Error,
	          'invalid UTF-8 sequence',
	          true,
	          1007,
	          'WS_ERR_INVALID_UTF8'
	        );

	        cb(error);
	        return;
	      }

	      if (this._state === INFLATING || this._allowSynchronousEvents) {
	        this.emit('message', buf, false);
	        this._state = GET_INFO;
	      } else {
	        this._state = DEFER_EVENT;
	        setImmediate(() => {
	          this.emit('message', buf, false);
	          this._state = GET_INFO;
	          this.startLoop(cb);
	        });
	      }
	    }
	  }

	  /**
	   * Handles a control message.
	   *
	   * @param {Buffer} data Data to handle
	   * @return {(Error|RangeError|undefined)} A possible error
	   * @private
	   */
	  controlMessage(data, cb) {
	    if (this._opcode === 0x08) {
	      if (data.length === 0) {
	        this._loop = false;
	        this.emit('conclude', 1005, EMPTY_BUFFER);
	        this.end();
	      } else {
	        const code = data.readUInt16BE(0);

	        if (!isValidStatusCode(code)) {
	          const error = this.createError(
	            RangeError,
	            `invalid status code ${code}`,
	            true,
	            1002,
	            'WS_ERR_INVALID_CLOSE_CODE'
	          );

	          cb(error);
	          return;
	        }

	        const buf = new FastBuffer(
	          data.buffer,
	          data.byteOffset + 2,
	          data.length - 2
	        );

	        if (!this._skipUTF8Validation && !isValidUTF8(buf)) {
	          const error = this.createError(
	            Error,
	            'invalid UTF-8 sequence',
	            true,
	            1007,
	            'WS_ERR_INVALID_UTF8'
	          );

	          cb(error);
	          return;
	        }

	        this._loop = false;
	        this.emit('conclude', code, buf);
	        this.end();
	      }

	      this._state = GET_INFO;
	      return;
	    }

	    if (this._allowSynchronousEvents) {
	      this.emit(this._opcode === 0x09 ? 'ping' : 'pong', data);
	      this._state = GET_INFO;
	    } else {
	      this._state = DEFER_EVENT;
	      setImmediate(() => {
	        this.emit(this._opcode === 0x09 ? 'ping' : 'pong', data);
	        this._state = GET_INFO;
	        this.startLoop(cb);
	      });
	    }
	  }

	  /**
	   * Builds an error object.
	   *
	   * @param {function(new:Error|RangeError)} ErrorCtor The error constructor
	   * @param {String} message The error message
	   * @param {Boolean} prefix Specifies whether or not to add a default prefix to
	   *     `message`
	   * @param {Number} statusCode The status code
	   * @param {String} errorCode The exposed error code
	   * @return {(Error|RangeError)} The error
	   * @private
	   */
	  createError(ErrorCtor, message, prefix, statusCode, errorCode) {
	    this._loop = false;
	    this._errored = true;

	    const err = new ErrorCtor(
	      prefix ? `Invalid WebSocket frame: ${message}` : message
	    );

	    Error.captureStackTrace(err, this.createError);
	    err.code = errorCode;
	    err[kStatusCode] = statusCode;
	    return err;
	  }
	}

	receiver = Receiver;
	return receiver;
}

/* eslint no-unused-vars: ["error", { "varsIgnorePattern": "^Duplex" }] */

var sender;
var hasRequiredSender;

function requireSender () {
	if (hasRequiredSender) return sender;
	hasRequiredSender = 1;

	const { Duplex } = require$$0$2;
	const { randomFillSync } = require$$1;

	const PerMessageDeflate = requirePermessageDeflate();
	const { EMPTY_BUFFER, kWebSocket, NOOP } = requireConstants();
	const { isBlob, isValidStatusCode } = requireValidation();
	const { mask: applyMask, toBuffer } = requireBufferUtil();

	const kByteLength = Symbol('kByteLength');
	const maskBuffer = Buffer.alloc(4);
	const RANDOM_POOL_SIZE = 8 * 1024;
	let randomPool;
	let randomPoolPointer = RANDOM_POOL_SIZE;

	const DEFAULT = 0;
	const DEFLATING = 1;
	const GET_BLOB_DATA = 2;

	/**
	 * HyBi Sender implementation.
	 */
	class Sender {
	  /**
	   * Creates a Sender instance.
	   *
	   * @param {Duplex} socket The connection socket
	   * @param {Object} [extensions] An object containing the negotiated extensions
	   * @param {Function} [generateMask] The function used to generate the masking
	   *     key
	   */
	  constructor(socket, extensions, generateMask) {
	    this._extensions = extensions || {};

	    if (generateMask) {
	      this._generateMask = generateMask;
	      this._maskBuffer = Buffer.alloc(4);
	    }

	    this._socket = socket;

	    this._firstFragment = true;
	    this._compress = false;

	    this._bufferedBytes = 0;
	    this._queue = [];
	    this._state = DEFAULT;
	    this.onerror = NOOP;
	    this[kWebSocket] = undefined;
	  }

	  /**
	   * Frames a piece of data according to the HyBi WebSocket protocol.
	   *
	   * @param {(Buffer|String)} data The data to frame
	   * @param {Object} options Options object
	   * @param {Boolean} [options.fin=false] Specifies whether or not to set the
	   *     FIN bit
	   * @param {Function} [options.generateMask] The function used to generate the
	   *     masking key
	   * @param {Boolean} [options.mask=false] Specifies whether or not to mask
	   *     `data`
	   * @param {Buffer} [options.maskBuffer] The buffer used to store the masking
	   *     key
	   * @param {Number} options.opcode The opcode
	   * @param {Boolean} [options.readOnly=false] Specifies whether `data` can be
	   *     modified
	   * @param {Boolean} [options.rsv1=false] Specifies whether or not to set the
	   *     RSV1 bit
	   * @return {(Buffer|String)[]} The framed data
	   * @public
	   */
	  static frame(data, options) {
	    let mask;
	    let merge = false;
	    let offset = 2;
	    let skipMasking = false;

	    if (options.mask) {
	      mask = options.maskBuffer || maskBuffer;

	      if (options.generateMask) {
	        options.generateMask(mask);
	      } else {
	        if (randomPoolPointer === RANDOM_POOL_SIZE) {
	          /* istanbul ignore else  */
	          if (randomPool === undefined) {
	            //
	            // This is lazily initialized because server-sent frames must not
	            // be masked so it may never be used.
	            //
	            randomPool = Buffer.alloc(RANDOM_POOL_SIZE);
	          }

	          randomFillSync(randomPool, 0, RANDOM_POOL_SIZE);
	          randomPoolPointer = 0;
	        }

	        mask[0] = randomPool[randomPoolPointer++];
	        mask[1] = randomPool[randomPoolPointer++];
	        mask[2] = randomPool[randomPoolPointer++];
	        mask[3] = randomPool[randomPoolPointer++];
	      }

	      skipMasking = (mask[0] | mask[1] | mask[2] | mask[3]) === 0;
	      offset = 6;
	    }

	    let dataLength;

	    if (typeof data === 'string') {
	      if (
	        (!options.mask || skipMasking) &&
	        options[kByteLength] !== undefined
	      ) {
	        dataLength = options[kByteLength];
	      } else {
	        data = Buffer.from(data);
	        dataLength = data.length;
	      }
	    } else {
	      dataLength = data.length;
	      merge = options.mask && options.readOnly && !skipMasking;
	    }

	    let payloadLength = dataLength;

	    if (dataLength >= 65536) {
	      offset += 8;
	      payloadLength = 127;
	    } else if (dataLength > 125) {
	      offset += 2;
	      payloadLength = 126;
	    }

	    const target = Buffer.allocUnsafe(merge ? dataLength + offset : offset);

	    target[0] = options.fin ? options.opcode | 0x80 : options.opcode;
	    if (options.rsv1) target[0] |= 0x40;

	    target[1] = payloadLength;

	    if (payloadLength === 126) {
	      target.writeUInt16BE(dataLength, 2);
	    } else if (payloadLength === 127) {
	      target[2] = target[3] = 0;
	      target.writeUIntBE(dataLength, 4, 6);
	    }

	    if (!options.mask) return [target, data];

	    target[1] |= 0x80;
	    target[offset - 4] = mask[0];
	    target[offset - 3] = mask[1];
	    target[offset - 2] = mask[2];
	    target[offset - 1] = mask[3];

	    if (skipMasking) return [target, data];

	    if (merge) {
	      applyMask(data, mask, target, offset, dataLength);
	      return [target];
	    }

	    applyMask(data, mask, data, 0, dataLength);
	    return [target, data];
	  }

	  /**
	   * Sends a close message to the other peer.
	   *
	   * @param {Number} [code] The status code component of the body
	   * @param {(String|Buffer)} [data] The message component of the body
	   * @param {Boolean} [mask=false] Specifies whether or not to mask the message
	   * @param {Function} [cb] Callback
	   * @public
	   */
	  close(code, data, mask, cb) {
	    let buf;

	    if (code === undefined) {
	      buf = EMPTY_BUFFER;
	    } else if (typeof code !== 'number' || !isValidStatusCode(code)) {
	      throw new TypeError('First argument must be a valid error code number');
	    } else if (data === undefined || !data.length) {
	      buf = Buffer.allocUnsafe(2);
	      buf.writeUInt16BE(code, 0);
	    } else {
	      const length = Buffer.byteLength(data);

	      if (length > 123) {
	        throw new RangeError('The message must not be greater than 123 bytes');
	      }

	      buf = Buffer.allocUnsafe(2 + length);
	      buf.writeUInt16BE(code, 0);

	      if (typeof data === 'string') {
	        buf.write(data, 2);
	      } else {
	        buf.set(data, 2);
	      }
	    }

	    const options = {
	      [kByteLength]: buf.length,
	      fin: true,
	      generateMask: this._generateMask,
	      mask,
	      maskBuffer: this._maskBuffer,
	      opcode: 0x08,
	      readOnly: false,
	      rsv1: false
	    };

	    if (this._state !== DEFAULT) {
	      this.enqueue([this.dispatch, buf, false, options, cb]);
	    } else {
	      this.sendFrame(Sender.frame(buf, options), cb);
	    }
	  }

	  /**
	   * Sends a ping message to the other peer.
	   *
	   * @param {*} data The message to send
	   * @param {Boolean} [mask=false] Specifies whether or not to mask `data`
	   * @param {Function} [cb] Callback
	   * @public
	   */
	  ping(data, mask, cb) {
	    let byteLength;
	    let readOnly;

	    if (typeof data === 'string') {
	      byteLength = Buffer.byteLength(data);
	      readOnly = false;
	    } else if (isBlob(data)) {
	      byteLength = data.size;
	      readOnly = false;
	    } else {
	      data = toBuffer(data);
	      byteLength = data.length;
	      readOnly = toBuffer.readOnly;
	    }

	    if (byteLength > 125) {
	      throw new RangeError('The data size must not be greater than 125 bytes');
	    }

	    const options = {
	      [kByteLength]: byteLength,
	      fin: true,
	      generateMask: this._generateMask,
	      mask,
	      maskBuffer: this._maskBuffer,
	      opcode: 0x09,
	      readOnly,
	      rsv1: false
	    };

	    if (isBlob(data)) {
	      if (this._state !== DEFAULT) {
	        this.enqueue([this.getBlobData, data, false, options, cb]);
	      } else {
	        this.getBlobData(data, false, options, cb);
	      }
	    } else if (this._state !== DEFAULT) {
	      this.enqueue([this.dispatch, data, false, options, cb]);
	    } else {
	      this.sendFrame(Sender.frame(data, options), cb);
	    }
	  }

	  /**
	   * Sends a pong message to the other peer.
	   *
	   * @param {*} data The message to send
	   * @param {Boolean} [mask=false] Specifies whether or not to mask `data`
	   * @param {Function} [cb] Callback
	   * @public
	   */
	  pong(data, mask, cb) {
	    let byteLength;
	    let readOnly;

	    if (typeof data === 'string') {
	      byteLength = Buffer.byteLength(data);
	      readOnly = false;
	    } else if (isBlob(data)) {
	      byteLength = data.size;
	      readOnly = false;
	    } else {
	      data = toBuffer(data);
	      byteLength = data.length;
	      readOnly = toBuffer.readOnly;
	    }

	    if (byteLength > 125) {
	      throw new RangeError('The data size must not be greater than 125 bytes');
	    }

	    const options = {
	      [kByteLength]: byteLength,
	      fin: true,
	      generateMask: this._generateMask,
	      mask,
	      maskBuffer: this._maskBuffer,
	      opcode: 0x0a,
	      readOnly,
	      rsv1: false
	    };

	    if (isBlob(data)) {
	      if (this._state !== DEFAULT) {
	        this.enqueue([this.getBlobData, data, false, options, cb]);
	      } else {
	        this.getBlobData(data, false, options, cb);
	      }
	    } else if (this._state !== DEFAULT) {
	      this.enqueue([this.dispatch, data, false, options, cb]);
	    } else {
	      this.sendFrame(Sender.frame(data, options), cb);
	    }
	  }

	  /**
	   * Sends a data message to the other peer.
	   *
	   * @param {*} data The message to send
	   * @param {Object} options Options object
	   * @param {Boolean} [options.binary=false] Specifies whether `data` is binary
	   *     or text
	   * @param {Boolean} [options.compress=false] Specifies whether or not to
	   *     compress `data`
	   * @param {Boolean} [options.fin=false] Specifies whether the fragment is the
	   *     last one
	   * @param {Boolean} [options.mask=false] Specifies whether or not to mask
	   *     `data`
	   * @param {Function} [cb] Callback
	   * @public
	   */
	  send(data, options, cb) {
	    const perMessageDeflate = this._extensions[PerMessageDeflate.extensionName];
	    let opcode = options.binary ? 2 : 1;
	    let rsv1 = options.compress;

	    let byteLength;
	    let readOnly;

	    if (typeof data === 'string') {
	      byteLength = Buffer.byteLength(data);
	      readOnly = false;
	    } else if (isBlob(data)) {
	      byteLength = data.size;
	      readOnly = false;
	    } else {
	      data = toBuffer(data);
	      byteLength = data.length;
	      readOnly = toBuffer.readOnly;
	    }

	    if (this._firstFragment) {
	      this._firstFragment = false;
	      if (
	        rsv1 &&
	        perMessageDeflate &&
	        perMessageDeflate.params[
	          perMessageDeflate._isServer
	            ? 'server_no_context_takeover'
	            : 'client_no_context_takeover'
	        ]
	      ) {
	        rsv1 = byteLength >= perMessageDeflate._threshold;
	      }
	      this._compress = rsv1;
	    } else {
	      rsv1 = false;
	      opcode = 0;
	    }

	    if (options.fin) this._firstFragment = true;

	    const opts = {
	      [kByteLength]: byteLength,
	      fin: options.fin,
	      generateMask: this._generateMask,
	      mask: options.mask,
	      maskBuffer: this._maskBuffer,
	      opcode,
	      readOnly,
	      rsv1
	    };

	    if (isBlob(data)) {
	      if (this._state !== DEFAULT) {
	        this.enqueue([this.getBlobData, data, this._compress, opts, cb]);
	      } else {
	        this.getBlobData(data, this._compress, opts, cb);
	      }
	    } else if (this._state !== DEFAULT) {
	      this.enqueue([this.dispatch, data, this._compress, opts, cb]);
	    } else {
	      this.dispatch(data, this._compress, opts, cb);
	    }
	  }

	  /**
	   * Gets the contents of a blob as binary data.
	   *
	   * @param {Blob} blob The blob
	   * @param {Boolean} [compress=false] Specifies whether or not to compress
	   *     the data
	   * @param {Object} options Options object
	   * @param {Boolean} [options.fin=false] Specifies whether or not to set the
	   *     FIN bit
	   * @param {Function} [options.generateMask] The function used to generate the
	   *     masking key
	   * @param {Boolean} [options.mask=false] Specifies whether or not to mask
	   *     `data`
	   * @param {Buffer} [options.maskBuffer] The buffer used to store the masking
	   *     key
	   * @param {Number} options.opcode The opcode
	   * @param {Boolean} [options.readOnly=false] Specifies whether `data` can be
	   *     modified
	   * @param {Boolean} [options.rsv1=false] Specifies whether or not to set the
	   *     RSV1 bit
	   * @param {Function} [cb] Callback
	   * @private
	   */
	  getBlobData(blob, compress, options, cb) {
	    this._bufferedBytes += options[kByteLength];
	    this._state = GET_BLOB_DATA;

	    blob
	      .arrayBuffer()
	      .then((arrayBuffer) => {
	        if (this._socket.destroyed) {
	          const err = new Error(
	            'The socket was closed while the blob was being read'
	          );

	          //
	          // `callCallbacks` is called in the next tick to ensure that errors
	          // that might be thrown in the callbacks behave like errors thrown
	          // outside the promise chain.
	          //
	          process.nextTick(callCallbacks, this, err, cb);
	          return;
	        }

	        this._bufferedBytes -= options[kByteLength];
	        const data = toBuffer(arrayBuffer);

	        if (!compress) {
	          this._state = DEFAULT;
	          this.sendFrame(Sender.frame(data, options), cb);
	          this.dequeue();
	        } else {
	          this.dispatch(data, compress, options, cb);
	        }
	      })
	      .catch((err) => {
	        //
	        // `onError` is called in the next tick for the same reason that
	        // `callCallbacks` above is.
	        //
	        process.nextTick(onError, this, err, cb);
	      });
	  }

	  /**
	   * Dispatches a message.
	   *
	   * @param {(Buffer|String)} data The message to send
	   * @param {Boolean} [compress=false] Specifies whether or not to compress
	   *     `data`
	   * @param {Object} options Options object
	   * @param {Boolean} [options.fin=false] Specifies whether or not to set the
	   *     FIN bit
	   * @param {Function} [options.generateMask] The function used to generate the
	   *     masking key
	   * @param {Boolean} [options.mask=false] Specifies whether or not to mask
	   *     `data`
	   * @param {Buffer} [options.maskBuffer] The buffer used to store the masking
	   *     key
	   * @param {Number} options.opcode The opcode
	   * @param {Boolean} [options.readOnly=false] Specifies whether `data` can be
	   *     modified
	   * @param {Boolean} [options.rsv1=false] Specifies whether or not to set the
	   *     RSV1 bit
	   * @param {Function} [cb] Callback
	   * @private
	   */
	  dispatch(data, compress, options, cb) {
	    if (!compress) {
	      this.sendFrame(Sender.frame(data, options), cb);
	      return;
	    }

	    const perMessageDeflate = this._extensions[PerMessageDeflate.extensionName];

	    this._bufferedBytes += options[kByteLength];
	    this._state = DEFLATING;
	    perMessageDeflate.compress(data, options.fin, (_, buf) => {
	      if (this._socket.destroyed) {
	        const err = new Error(
	          'The socket was closed while data was being compressed'
	        );

	        callCallbacks(this, err, cb);
	        return;
	      }

	      this._bufferedBytes -= options[kByteLength];
	      this._state = DEFAULT;
	      options.readOnly = false;
	      this.sendFrame(Sender.frame(buf, options), cb);
	      this.dequeue();
	    });
	  }

	  /**
	   * Executes queued send operations.
	   *
	   * @private
	   */
	  dequeue() {
	    while (this._state === DEFAULT && this._queue.length) {
	      const params = this._queue.shift();

	      this._bufferedBytes -= params[3][kByteLength];
	      Reflect.apply(params[0], this, params.slice(1));
	    }
	  }

	  /**
	   * Enqueues a send operation.
	   *
	   * @param {Array} params Send operation parameters.
	   * @private
	   */
	  enqueue(params) {
	    this._bufferedBytes += params[3][kByteLength];
	    this._queue.push(params);
	  }

	  /**
	   * Sends a frame.
	   *
	   * @param {(Buffer | String)[]} list The frame to send
	   * @param {Function} [cb] Callback
	   * @private
	   */
	  sendFrame(list, cb) {
	    if (list.length === 2) {
	      this._socket.cork();
	      this._socket.write(list[0]);
	      this._socket.write(list[1], cb);
	      this._socket.uncork();
	    } else {
	      this._socket.write(list[0], cb);
	    }
	  }
	}

	sender = Sender;

	/**
	 * Calls queued callbacks with an error.
	 *
	 * @param {Sender} sender The `Sender` instance
	 * @param {Error} err The error to call the callbacks with
	 * @param {Function} [cb] The first callback
	 * @private
	 */
	function callCallbacks(sender, err, cb) {
	  if (typeof cb === 'function') cb(err);

	  for (let i = 0; i < sender._queue.length; i++) {
	    const params = sender._queue[i];
	    const callback = params[params.length - 1];

	    if (typeof callback === 'function') callback(err);
	  }
	}

	/**
	 * Handles a `Sender` error.
	 *
	 * @param {Sender} sender The `Sender` instance
	 * @param {Error} err The error
	 * @param {Function} [cb] The first pending callback
	 * @private
	 */
	function onError(sender, err, cb) {
	  callCallbacks(sender, err, cb);
	  sender.onerror(err);
	}
	return sender;
}

var eventTarget;
var hasRequiredEventTarget;

function requireEventTarget () {
	if (hasRequiredEventTarget) return eventTarget;
	hasRequiredEventTarget = 1;

	const { kForOnEventAttribute, kListener } = requireConstants();

	const kCode = Symbol('kCode');
	const kData = Symbol('kData');
	const kError = Symbol('kError');
	const kMessage = Symbol('kMessage');
	const kReason = Symbol('kReason');
	const kTarget = Symbol('kTarget');
	const kType = Symbol('kType');
	const kWasClean = Symbol('kWasClean');

	/**
	 * Class representing an event.
	 */
	class Event {
	  /**
	   * Create a new `Event`.
	   *
	   * @param {String} type The name of the event
	   * @throws {TypeError} If the `type` argument is not specified
	   */
	  constructor(type) {
	    this[kTarget] = null;
	    this[kType] = type;
	  }

	  /**
	   * @type {*}
	   */
	  get target() {
	    return this[kTarget];
	  }

	  /**
	   * @type {String}
	   */
	  get type() {
	    return this[kType];
	  }
	}

	Object.defineProperty(Event.prototype, 'target', { enumerable: true });
	Object.defineProperty(Event.prototype, 'type', { enumerable: true });

	/**
	 * Class representing a close event.
	 *
	 * @extends Event
	 */
	class CloseEvent extends Event {
	  /**
	   * Create a new `CloseEvent`.
	   *
	   * @param {String} type The name of the event
	   * @param {Object} [options] A dictionary object that allows for setting
	   *     attributes via object members of the same name
	   * @param {Number} [options.code=0] The status code explaining why the
	   *     connection was closed
	   * @param {String} [options.reason=''] A human-readable string explaining why
	   *     the connection was closed
	   * @param {Boolean} [options.wasClean=false] Indicates whether or not the
	   *     connection was cleanly closed
	   */
	  constructor(type, options = {}) {
	    super(type);

	    this[kCode] = options.code === undefined ? 0 : options.code;
	    this[kReason] = options.reason === undefined ? '' : options.reason;
	    this[kWasClean] = options.wasClean === undefined ? false : options.wasClean;
	  }

	  /**
	   * @type {Number}
	   */
	  get code() {
	    return this[kCode];
	  }

	  /**
	   * @type {String}
	   */
	  get reason() {
	    return this[kReason];
	  }

	  /**
	   * @type {Boolean}
	   */
	  get wasClean() {
	    return this[kWasClean];
	  }
	}

	Object.defineProperty(CloseEvent.prototype, 'code', { enumerable: true });
	Object.defineProperty(CloseEvent.prototype, 'reason', { enumerable: true });
	Object.defineProperty(CloseEvent.prototype, 'wasClean', { enumerable: true });

	/**
	 * Class representing an error event.
	 *
	 * @extends Event
	 */
	class ErrorEvent extends Event {
	  /**
	   * Create a new `ErrorEvent`.
	   *
	   * @param {String} type The name of the event
	   * @param {Object} [options] A dictionary object that allows for setting
	   *     attributes via object members of the same name
	   * @param {*} [options.error=null] The error that generated this event
	   * @param {String} [options.message=''] The error message
	   */
	  constructor(type, options = {}) {
	    super(type);

	    this[kError] = options.error === undefined ? null : options.error;
	    this[kMessage] = options.message === undefined ? '' : options.message;
	  }

	  /**
	   * @type {*}
	   */
	  get error() {
	    return this[kError];
	  }

	  /**
	   * @type {String}
	   */
	  get message() {
	    return this[kMessage];
	  }
	}

	Object.defineProperty(ErrorEvent.prototype, 'error', { enumerable: true });
	Object.defineProperty(ErrorEvent.prototype, 'message', { enumerable: true });

	/**
	 * Class representing a message event.
	 *
	 * @extends Event
	 */
	class MessageEvent extends Event {
	  /**
	   * Create a new `MessageEvent`.
	   *
	   * @param {String} type The name of the event
	   * @param {Object} [options] A dictionary object that allows for setting
	   *     attributes via object members of the same name
	   * @param {*} [options.data=null] The message content
	   */
	  constructor(type, options = {}) {
	    super(type);

	    this[kData] = options.data === undefined ? null : options.data;
	  }

	  /**
	   * @type {*}
	   */
	  get data() {
	    return this[kData];
	  }
	}

	Object.defineProperty(MessageEvent.prototype, 'data', { enumerable: true });

	/**
	 * This provides methods for emulating the `EventTarget` interface. It's not
	 * meant to be used directly.
	 *
	 * @mixin
	 */
	const EventTarget = {
	  /**
	   * Register an event listener.
	   *
	   * @param {String} type A string representing the event type to listen for
	   * @param {(Function|Object)} handler The listener to add
	   * @param {Object} [options] An options object specifies characteristics about
	   *     the event listener
	   * @param {Boolean} [options.once=false] A `Boolean` indicating that the
	   *     listener should be invoked at most once after being added. If `true`,
	   *     the listener would be automatically removed when invoked.
	   * @public
	   */
	  addEventListener(type, handler, options = {}) {
	    for (const listener of this.listeners(type)) {
	      if (
	        !options[kForOnEventAttribute] &&
	        listener[kListener] === handler &&
	        !listener[kForOnEventAttribute]
	      ) {
	        return;
	      }
	    }

	    let wrapper;

	    if (type === 'message') {
	      wrapper = function onMessage(data, isBinary) {
	        const event = new MessageEvent('message', {
	          data: isBinary ? data : data.toString()
	        });

	        event[kTarget] = this;
	        callListener(handler, this, event);
	      };
	    } else if (type === 'close') {
	      wrapper = function onClose(code, message) {
	        const event = new CloseEvent('close', {
	          code,
	          reason: message.toString(),
	          wasClean: this._closeFrameReceived && this._closeFrameSent
	        });

	        event[kTarget] = this;
	        callListener(handler, this, event);
	      };
	    } else if (type === 'error') {
	      wrapper = function onError(error) {
	        const event = new ErrorEvent('error', {
	          error,
	          message: error.message
	        });

	        event[kTarget] = this;
	        callListener(handler, this, event);
	      };
	    } else if (type === 'open') {
	      wrapper = function onOpen() {
	        const event = new Event('open');

	        event[kTarget] = this;
	        callListener(handler, this, event);
	      };
	    } else {
	      return;
	    }

	    wrapper[kForOnEventAttribute] = !!options[kForOnEventAttribute];
	    wrapper[kListener] = handler;

	    if (options.once) {
	      this.once(type, wrapper);
	    } else {
	      this.on(type, wrapper);
	    }
	  },

	  /**
	   * Remove an event listener.
	   *
	   * @param {String} type A string representing the event type to remove
	   * @param {(Function|Object)} handler The listener to remove
	   * @public
	   */
	  removeEventListener(type, handler) {
	    for (const listener of this.listeners(type)) {
	      if (listener[kListener] === handler && !listener[kForOnEventAttribute]) {
	        this.removeListener(type, listener);
	        break;
	      }
	    }
	  }
	};

	eventTarget = {
	  CloseEvent,
	  ErrorEvent,
	  Event,
	  EventTarget,
	  MessageEvent
	};

	/**
	 * Call an event listener
	 *
	 * @param {(Function|Object)} listener The listener to call
	 * @param {*} thisArg The value to use as `this`` when calling the listener
	 * @param {Event} event The event to pass to the listener
	 * @private
	 */
	function callListener(listener, thisArg, event) {
	  if (typeof listener === 'object' && listener.handleEvent) {
	    listener.handleEvent.call(listener, event);
	  } else {
	    listener.call(thisArg, event);
	  }
	}
	return eventTarget;
}

var extension;
var hasRequiredExtension;

function requireExtension () {
	if (hasRequiredExtension) return extension;
	hasRequiredExtension = 1;

	const { tokenChars } = requireValidation();

	/**
	 * Adds an offer to the map of extension offers or a parameter to the map of
	 * parameters.
	 *
	 * @param {Object} dest The map of extension offers or parameters
	 * @param {String} name The extension or parameter name
	 * @param {(Object|Boolean|String)} elem The extension parameters or the
	 *     parameter value
	 * @private
	 */
	function push(dest, name, elem) {
	  if (dest[name] === undefined) dest[name] = [elem];
	  else dest[name].push(elem);
	}

	/**
	 * Parses the `Sec-WebSocket-Extensions` header into an object.
	 *
	 * @param {String} header The field value of the header
	 * @return {Object} The parsed object
	 * @public
	 */
	function parse(header) {
	  const offers = Object.create(null);
	  let params = Object.create(null);
	  let mustUnescape = false;
	  let isEscaping = false;
	  let inQuotes = false;
	  let extensionName;
	  let paramName;
	  let start = -1;
	  let code = -1;
	  let end = -1;
	  let i = 0;

	  for (; i < header.length; i++) {
	    code = header.charCodeAt(i);

	    if (extensionName === undefined) {
	      if (end === -1 && tokenChars[code] === 1) {
	        if (start === -1) start = i;
	      } else if (
	        i !== 0 &&
	        (code === 0x20 /* ' ' */ || code === 0x09) /* '\t' */
	      ) {
	        if (end === -1 && start !== -1) end = i;
	      } else if (code === 0x3b /* ';' */ || code === 0x2c /* ',' */) {
	        if (start === -1) {
	          throw new SyntaxError(`Unexpected character at index ${i}`);
	        }

	        if (end === -1) end = i;
	        const name = header.slice(start, end);
	        if (code === 0x2c) {
	          push(offers, name, params);
	          params = Object.create(null);
	        } else {
	          extensionName = name;
	        }

	        start = end = -1;
	      } else {
	        throw new SyntaxError(`Unexpected character at index ${i}`);
	      }
	    } else if (paramName === undefined) {
	      if (end === -1 && tokenChars[code] === 1) {
	        if (start === -1) start = i;
	      } else if (code === 0x20 || code === 0x09) {
	        if (end === -1 && start !== -1) end = i;
	      } else if (code === 0x3b || code === 0x2c) {
	        if (start === -1) {
	          throw new SyntaxError(`Unexpected character at index ${i}`);
	        }

	        if (end === -1) end = i;
	        push(params, header.slice(start, end), true);
	        if (code === 0x2c) {
	          push(offers, extensionName, params);
	          params = Object.create(null);
	          extensionName = undefined;
	        }

	        start = end = -1;
	      } else if (code === 0x3d /* '=' */ && start !== -1 && end === -1) {
	        paramName = header.slice(start, i);
	        start = end = -1;
	      } else {
	        throw new SyntaxError(`Unexpected character at index ${i}`);
	      }
	    } else {
	      //
	      // The value of a quoted-string after unescaping must conform to the
	      // token ABNF, so only token characters are valid.
	      // Ref: https://tools.ietf.org/html/rfc6455#section-9.1
	      //
	      if (isEscaping) {
	        if (tokenChars[code] !== 1) {
	          throw new SyntaxError(`Unexpected character at index ${i}`);
	        }
	        if (start === -1) start = i;
	        else if (!mustUnescape) mustUnescape = true;
	        isEscaping = false;
	      } else if (inQuotes) {
	        if (tokenChars[code] === 1) {
	          if (start === -1) start = i;
	        } else if (code === 0x22 /* '"' */ && start !== -1) {
	          inQuotes = false;
	          end = i;
	        } else if (code === 0x5c /* '\' */) {
	          isEscaping = true;
	        } else {
	          throw new SyntaxError(`Unexpected character at index ${i}`);
	        }
	      } else if (code === 0x22 && header.charCodeAt(i - 1) === 0x3d) {
	        inQuotes = true;
	      } else if (end === -1 && tokenChars[code] === 1) {
	        if (start === -1) start = i;
	      } else if (start !== -1 && (code === 0x20 || code === 0x09)) {
	        if (end === -1) end = i;
	      } else if (code === 0x3b || code === 0x2c) {
	        if (start === -1) {
	          throw new SyntaxError(`Unexpected character at index ${i}`);
	        }

	        if (end === -1) end = i;
	        let value = header.slice(start, end);
	        if (mustUnescape) {
	          value = value.replace(/\\/g, '');
	          mustUnescape = false;
	        }
	        push(params, paramName, value);
	        if (code === 0x2c) {
	          push(offers, extensionName, params);
	          params = Object.create(null);
	          extensionName = undefined;
	        }

	        paramName = undefined;
	        start = end = -1;
	      } else {
	        throw new SyntaxError(`Unexpected character at index ${i}`);
	      }
	    }
	  }

	  if (start === -1 || inQuotes || code === 0x20 || code === 0x09) {
	    throw new SyntaxError('Unexpected end of input');
	  }

	  if (end === -1) end = i;
	  const token = header.slice(start, end);
	  if (extensionName === undefined) {
	    push(offers, token, params);
	  } else {
	    if (paramName === undefined) {
	      push(params, token, true);
	    } else if (mustUnescape) {
	      push(params, paramName, token.replace(/\\/g, ''));
	    } else {
	      push(params, paramName, token);
	    }
	    push(offers, extensionName, params);
	  }

	  return offers;
	}

	/**
	 * Builds the `Sec-WebSocket-Extensions` header field value.
	 *
	 * @param {Object} extensions The map of extensions and parameters to format
	 * @return {String} A string representing the given object
	 * @public
	 */
	function format(extensions) {
	  return Object.keys(extensions)
	    .map((extension) => {
	      let configurations = extensions[extension];
	      if (!Array.isArray(configurations)) configurations = [configurations];
	      return configurations
	        .map((params) => {
	          return [extension]
	            .concat(
	              Object.keys(params).map((k) => {
	                let values = params[k];
	                if (!Array.isArray(values)) values = [values];
	                return values
	                  .map((v) => (v === true ? k : `${k}=${v}`))
	                  .join('; ');
	              })
	            )
	            .join('; ');
	        })
	        .join(', ');
	    })
	    .join(', ');
	}

	extension = { format, parse };
	return extension;
}

/* eslint no-unused-vars: ["error", { "varsIgnorePattern": "^Duplex|Readable$", "caughtErrors": "none" }] */

var websocket;
var hasRequiredWebsocket;

function requireWebsocket () {
	if (hasRequiredWebsocket) return websocket;
	hasRequiredWebsocket = 1;

	const EventEmitter = require$$0$3;
	const https = require$$1$1;
	const http = require$$2;
	const net = require$$3;
	const tls = require$$4;
	const { randomBytes, createHash } = require$$1;
	const { Duplex, Readable } = require$$0$2;
	const { URL } = require$$7;

	const PerMessageDeflate = requirePermessageDeflate();
	const Receiver = requireReceiver();
	const Sender = requireSender();
	const { isBlob } = requireValidation();

	const {
	  BINARY_TYPES,
	  CLOSE_TIMEOUT,
	  EMPTY_BUFFER,
	  GUID,
	  kForOnEventAttribute,
	  kListener,
	  kStatusCode,
	  kWebSocket,
	  NOOP
	} = requireConstants();
	const {
	  EventTarget: { addEventListener, removeEventListener }
	} = requireEventTarget();
	const { format, parse } = requireExtension();
	const { toBuffer } = requireBufferUtil();

	const kAborted = Symbol('kAborted');
	const protocolVersions = [8, 13];
	const readyStates = ['CONNECTING', 'OPEN', 'CLOSING', 'CLOSED'];
	const subprotocolRegex = /^[!#$%&'*+\-.0-9A-Z^_`|a-z~]+$/;

	/**
	 * Class representing a WebSocket.
	 *
	 * @extends EventEmitter
	 */
	class WebSocket extends EventEmitter {
	  /**
	   * Create a new `WebSocket`.
	   *
	   * @param {(String|URL)} address The URL to which to connect
	   * @param {(String|String[])} [protocols] The subprotocols
	   * @param {Object} [options] Connection options
	   */
	  constructor(address, protocols, options) {
	    super();

	    this._binaryType = BINARY_TYPES[0];
	    this._closeCode = 1006;
	    this._closeFrameReceived = false;
	    this._closeFrameSent = false;
	    this._closeMessage = EMPTY_BUFFER;
	    this._closeTimer = null;
	    this._errorEmitted = false;
	    this._extensions = {};
	    this._paused = false;
	    this._protocol = '';
	    this._readyState = WebSocket.CONNECTING;
	    this._receiver = null;
	    this._sender = null;
	    this._socket = null;

	    if (address !== null) {
	      this._bufferedAmount = 0;
	      this._isServer = false;
	      this._redirects = 0;

	      if (protocols === undefined) {
	        protocols = [];
	      } else if (!Array.isArray(protocols)) {
	        if (typeof protocols === 'object' && protocols !== null) {
	          options = protocols;
	          protocols = [];
	        } else {
	          protocols = [protocols];
	        }
	      }

	      initAsClient(this, address, protocols, options);
	    } else {
	      this._autoPong = options.autoPong;
	      this._closeTimeout = options.closeTimeout;
	      this._isServer = true;
	    }
	  }

	  /**
	   * For historical reasons, the custom "nodebuffer" type is used by the default
	   * instead of "blob".
	   *
	   * @type {String}
	   */
	  get binaryType() {
	    return this._binaryType;
	  }

	  set binaryType(type) {
	    if (!BINARY_TYPES.includes(type)) return;

	    this._binaryType = type;

	    //
	    // Allow to change `binaryType` on the fly.
	    //
	    if (this._receiver) this._receiver._binaryType = type;
	  }

	  /**
	   * @type {Number}
	   */
	  get bufferedAmount() {
	    if (!this._socket) return this._bufferedAmount;

	    return this._socket._writableState.length + this._sender._bufferedBytes;
	  }

	  /**
	   * @type {String}
	   */
	  get extensions() {
	    return Object.keys(this._extensions).join();
	  }

	  /**
	   * @type {Boolean}
	   */
	  get isPaused() {
	    return this._paused;
	  }

	  /**
	   * @type {Function}
	   */
	  /* istanbul ignore next */
	  get onclose() {
	    return null;
	  }

	  /**
	   * @type {Function}
	   */
	  /* istanbul ignore next */
	  get onerror() {
	    return null;
	  }

	  /**
	   * @type {Function}
	   */
	  /* istanbul ignore next */
	  get onopen() {
	    return null;
	  }

	  /**
	   * @type {Function}
	   */
	  /* istanbul ignore next */
	  get onmessage() {
	    return null;
	  }

	  /**
	   * @type {String}
	   */
	  get protocol() {
	    return this._protocol;
	  }

	  /**
	   * @type {Number}
	   */
	  get readyState() {
	    return this._readyState;
	  }

	  /**
	   * @type {String}
	   */
	  get url() {
	    return this._url;
	  }

	  /**
	   * Set up the socket and the internal resources.
	   *
	   * @param {Duplex} socket The network socket between the server and client
	   * @param {Buffer} head The first packet of the upgraded stream
	   * @param {Object} options Options object
	   * @param {Boolean} [options.allowSynchronousEvents=false] Specifies whether
	   *     any of the `'message'`, `'ping'`, and `'pong'` events can be emitted
	   *     multiple times in the same tick
	   * @param {Function} [options.generateMask] The function used to generate the
	   *     masking key
	   * @param {Number} [options.maxPayload=0] The maximum allowed message size
	   * @param {Boolean} [options.skipUTF8Validation=false] Specifies whether or
	   *     not to skip UTF-8 validation for text and close messages
	   * @private
	   */
	  setSocket(socket, head, options) {
	    const receiver = new Receiver({
	      allowSynchronousEvents: options.allowSynchronousEvents,
	      binaryType: this.binaryType,
	      extensions: this._extensions,
	      isServer: this._isServer,
	      maxPayload: options.maxPayload,
	      skipUTF8Validation: options.skipUTF8Validation
	    });

	    const sender = new Sender(socket, this._extensions, options.generateMask);

	    this._receiver = receiver;
	    this._sender = sender;
	    this._socket = socket;

	    receiver[kWebSocket] = this;
	    sender[kWebSocket] = this;
	    socket[kWebSocket] = this;

	    receiver.on('conclude', receiverOnConclude);
	    receiver.on('drain', receiverOnDrain);
	    receiver.on('error', receiverOnError);
	    receiver.on('message', receiverOnMessage);
	    receiver.on('ping', receiverOnPing);
	    receiver.on('pong', receiverOnPong);

	    sender.onerror = senderOnError;

	    //
	    // These methods may not be available if `socket` is just a `Duplex`.
	    //
	    if (socket.setTimeout) socket.setTimeout(0);
	    if (socket.setNoDelay) socket.setNoDelay();

	    if (head.length > 0) socket.unshift(head);

	    socket.on('close', socketOnClose);
	    socket.on('data', socketOnData);
	    socket.on('end', socketOnEnd);
	    socket.on('error', socketOnError);

	    this._readyState = WebSocket.OPEN;
	    this.emit('open');
	  }

	  /**
	   * Emit the `'close'` event.
	   *
	   * @private
	   */
	  emitClose() {
	    if (!this._socket) {
	      this._readyState = WebSocket.CLOSED;
	      this.emit('close', this._closeCode, this._closeMessage);
	      return;
	    }

	    if (this._extensions[PerMessageDeflate.extensionName]) {
	      this._extensions[PerMessageDeflate.extensionName].cleanup();
	    }

	    this._receiver.removeAllListeners();
	    this._readyState = WebSocket.CLOSED;
	    this.emit('close', this._closeCode, this._closeMessage);
	  }

	  /**
	   * Start a closing handshake.
	   *
	   *          +----------+   +-----------+   +----------+
	   *     - - -|ws.close()|-->|close frame|-->|ws.close()|- - -
	   *    |     +----------+   +-----------+   +----------+     |
	   *          +----------+   +-----------+         |
	   * CLOSING  |ws.close()|<--|close frame|<--+-----+       CLOSING
	   *          +----------+   +-----------+   |
	   *    |           |                        |   +---+        |
	   *                +------------------------+-->|fin| - - - -
	   *    |         +---+                      |   +---+
	   *     - - - - -|fin|<---------------------+
	   *              +---+
	   *
	   * @param {Number} [code] Status code explaining why the connection is closing
	   * @param {(String|Buffer)} [data] The reason why the connection is
	   *     closing
	   * @public
	   */
	  close(code, data) {
	    if (this.readyState === WebSocket.CLOSED) return;
	    if (this.readyState === WebSocket.CONNECTING) {
	      const msg = 'WebSocket was closed before the connection was established';
	      abortHandshake(this, this._req, msg);
	      return;
	    }

	    if (this.readyState === WebSocket.CLOSING) {
	      if (
	        this._closeFrameSent &&
	        (this._closeFrameReceived || this._receiver._writableState.errorEmitted)
	      ) {
	        this._socket.end();
	      }

	      return;
	    }

	    this._readyState = WebSocket.CLOSING;
	    this._sender.close(code, data, !this._isServer, (err) => {
	      //
	      // This error is handled by the `'error'` listener on the socket. We only
	      // want to know if the close frame has been sent here.
	      //
	      if (err) return;

	      this._closeFrameSent = true;

	      if (
	        this._closeFrameReceived ||
	        this._receiver._writableState.errorEmitted
	      ) {
	        this._socket.end();
	      }
	    });

	    setCloseTimer(this);
	  }

	  /**
	   * Pause the socket.
	   *
	   * @public
	   */
	  pause() {
	    if (
	      this.readyState === WebSocket.CONNECTING ||
	      this.readyState === WebSocket.CLOSED
	    ) {
	      return;
	    }

	    this._paused = true;
	    this._socket.pause();
	  }

	  /**
	   * Send a ping.
	   *
	   * @param {*} [data] The data to send
	   * @param {Boolean} [mask] Indicates whether or not to mask `data`
	   * @param {Function} [cb] Callback which is executed when the ping is sent
	   * @public
	   */
	  ping(data, mask, cb) {
	    if (this.readyState === WebSocket.CONNECTING) {
	      throw new Error('WebSocket is not open: readyState 0 (CONNECTING)');
	    }

	    if (typeof data === 'function') {
	      cb = data;
	      data = mask = undefined;
	    } else if (typeof mask === 'function') {
	      cb = mask;
	      mask = undefined;
	    }

	    if (typeof data === 'number') data = data.toString();

	    if (this.readyState !== WebSocket.OPEN) {
	      sendAfterClose(this, data, cb);
	      return;
	    }

	    if (mask === undefined) mask = !this._isServer;
	    this._sender.ping(data || EMPTY_BUFFER, mask, cb);
	  }

	  /**
	   * Send a pong.
	   *
	   * @param {*} [data] The data to send
	   * @param {Boolean} [mask] Indicates whether or not to mask `data`
	   * @param {Function} [cb] Callback which is executed when the pong is sent
	   * @public
	   */
	  pong(data, mask, cb) {
	    if (this.readyState === WebSocket.CONNECTING) {
	      throw new Error('WebSocket is not open: readyState 0 (CONNECTING)');
	    }

	    if (typeof data === 'function') {
	      cb = data;
	      data = mask = undefined;
	    } else if (typeof mask === 'function') {
	      cb = mask;
	      mask = undefined;
	    }

	    if (typeof data === 'number') data = data.toString();

	    if (this.readyState !== WebSocket.OPEN) {
	      sendAfterClose(this, data, cb);
	      return;
	    }

	    if (mask === undefined) mask = !this._isServer;
	    this._sender.pong(data || EMPTY_BUFFER, mask, cb);
	  }

	  /**
	   * Resume the socket.
	   *
	   * @public
	   */
	  resume() {
	    if (
	      this.readyState === WebSocket.CONNECTING ||
	      this.readyState === WebSocket.CLOSED
	    ) {
	      return;
	    }

	    this._paused = false;
	    if (!this._receiver._writableState.needDrain) this._socket.resume();
	  }

	  /**
	   * Send a data message.
	   *
	   * @param {*} data The message to send
	   * @param {Object} [options] Options object
	   * @param {Boolean} [options.binary] Specifies whether `data` is binary or
	   *     text
	   * @param {Boolean} [options.compress] Specifies whether or not to compress
	   *     `data`
	   * @param {Boolean} [options.fin=true] Specifies whether the fragment is the
	   *     last one
	   * @param {Boolean} [options.mask] Specifies whether or not to mask `data`
	   * @param {Function} [cb] Callback which is executed when data is written out
	   * @public
	   */
	  send(data, options, cb) {
	    if (this.readyState === WebSocket.CONNECTING) {
	      throw new Error('WebSocket is not open: readyState 0 (CONNECTING)');
	    }

	    if (typeof options === 'function') {
	      cb = options;
	      options = {};
	    }

	    if (typeof data === 'number') data = data.toString();

	    if (this.readyState !== WebSocket.OPEN) {
	      sendAfterClose(this, data, cb);
	      return;
	    }

	    const opts = {
	      binary: typeof data !== 'string',
	      mask: !this._isServer,
	      compress: true,
	      fin: true,
	      ...options
	    };

	    if (!this._extensions[PerMessageDeflate.extensionName]) {
	      opts.compress = false;
	    }

	    this._sender.send(data || EMPTY_BUFFER, opts, cb);
	  }

	  /**
	   * Forcibly close the connection.
	   *
	   * @public
	   */
	  terminate() {
	    if (this.readyState === WebSocket.CLOSED) return;
	    if (this.readyState === WebSocket.CONNECTING) {
	      const msg = 'WebSocket was closed before the connection was established';
	      abortHandshake(this, this._req, msg);
	      return;
	    }

	    if (this._socket) {
	      this._readyState = WebSocket.CLOSING;
	      this._socket.destroy();
	    }
	  }
	}

	/**
	 * @constant {Number} CONNECTING
	 * @memberof WebSocket
	 */
	Object.defineProperty(WebSocket, 'CONNECTING', {
	  enumerable: true,
	  value: readyStates.indexOf('CONNECTING')
	});

	/**
	 * @constant {Number} CONNECTING
	 * @memberof WebSocket.prototype
	 */
	Object.defineProperty(WebSocket.prototype, 'CONNECTING', {
	  enumerable: true,
	  value: readyStates.indexOf('CONNECTING')
	});

	/**
	 * @constant {Number} OPEN
	 * @memberof WebSocket
	 */
	Object.defineProperty(WebSocket, 'OPEN', {
	  enumerable: true,
	  value: readyStates.indexOf('OPEN')
	});

	/**
	 * @constant {Number} OPEN
	 * @memberof WebSocket.prototype
	 */
	Object.defineProperty(WebSocket.prototype, 'OPEN', {
	  enumerable: true,
	  value: readyStates.indexOf('OPEN')
	});

	/**
	 * @constant {Number} CLOSING
	 * @memberof WebSocket
	 */
	Object.defineProperty(WebSocket, 'CLOSING', {
	  enumerable: true,
	  value: readyStates.indexOf('CLOSING')
	});

	/**
	 * @constant {Number} CLOSING
	 * @memberof WebSocket.prototype
	 */
	Object.defineProperty(WebSocket.prototype, 'CLOSING', {
	  enumerable: true,
	  value: readyStates.indexOf('CLOSING')
	});

	/**
	 * @constant {Number} CLOSED
	 * @memberof WebSocket
	 */
	Object.defineProperty(WebSocket, 'CLOSED', {
	  enumerable: true,
	  value: readyStates.indexOf('CLOSED')
	});

	/**
	 * @constant {Number} CLOSED
	 * @memberof WebSocket.prototype
	 */
	Object.defineProperty(WebSocket.prototype, 'CLOSED', {
	  enumerable: true,
	  value: readyStates.indexOf('CLOSED')
	});

	[
	  'binaryType',
	  'bufferedAmount',
	  'extensions',
	  'isPaused',
	  'protocol',
	  'readyState',
	  'url'
	].forEach((property) => {
	  Object.defineProperty(WebSocket.prototype, property, { enumerable: true });
	});

	//
	// Add the `onopen`, `onerror`, `onclose`, and `onmessage` attributes.
	// See https://html.spec.whatwg.org/multipage/comms.html#the-websocket-interface
	//
	['open', 'error', 'close', 'message'].forEach((method) => {
	  Object.defineProperty(WebSocket.prototype, `on${method}`, {
	    enumerable: true,
	    get() {
	      for (const listener of this.listeners(method)) {
	        if (listener[kForOnEventAttribute]) return listener[kListener];
	      }

	      return null;
	    },
	    set(handler) {
	      for (const listener of this.listeners(method)) {
	        if (listener[kForOnEventAttribute]) {
	          this.removeListener(method, listener);
	          break;
	        }
	      }

	      if (typeof handler !== 'function') return;

	      this.addEventListener(method, handler, {
	        [kForOnEventAttribute]: true
	      });
	    }
	  });
	});

	WebSocket.prototype.addEventListener = addEventListener;
	WebSocket.prototype.removeEventListener = removeEventListener;

	websocket = WebSocket;

	/**
	 * Initialize a WebSocket client.
	 *
	 * @param {WebSocket} websocket The client to initialize
	 * @param {(String|URL)} address The URL to which to connect
	 * @param {Array} protocols The subprotocols
	 * @param {Object} [options] Connection options
	 * @param {Boolean} [options.allowSynchronousEvents=true] Specifies whether any
	 *     of the `'message'`, `'ping'`, and `'pong'` events can be emitted multiple
	 *     times in the same tick
	 * @param {Boolean} [options.autoPong=true] Specifies whether or not to
	 *     automatically send a pong in response to a ping
	 * @param {Number} [options.closeTimeout=30000] Duration in milliseconds to wait
	 *     for the closing handshake to finish after `websocket.close()` is called
	 * @param {Function} [options.finishRequest] A function which can be used to
	 *     customize the headers of each http request before it is sent
	 * @param {Boolean} [options.followRedirects=false] Whether or not to follow
	 *     redirects
	 * @param {Function} [options.generateMask] The function used to generate the
	 *     masking key
	 * @param {Number} [options.handshakeTimeout] Timeout in milliseconds for the
	 *     handshake request
	 * @param {Number} [options.maxPayload=104857600] The maximum allowed message
	 *     size
	 * @param {Number} [options.maxRedirects=10] The maximum number of redirects
	 *     allowed
	 * @param {String} [options.origin] Value of the `Origin` or
	 *     `Sec-WebSocket-Origin` header
	 * @param {(Boolean|Object)} [options.perMessageDeflate=true] Enable/disable
	 *     permessage-deflate
	 * @param {Number} [options.protocolVersion=13] Value of the
	 *     `Sec-WebSocket-Version` header
	 * @param {Boolean} [options.skipUTF8Validation=false] Specifies whether or
	 *     not to skip UTF-8 validation for text and close messages
	 * @private
	 */
	function initAsClient(websocket, address, protocols, options) {
	  const opts = {
	    allowSynchronousEvents: true,
	    autoPong: true,
	    closeTimeout: CLOSE_TIMEOUT,
	    protocolVersion: protocolVersions[1],
	    maxPayload: 100 * 1024 * 1024,
	    skipUTF8Validation: false,
	    perMessageDeflate: true,
	    followRedirects: false,
	    maxRedirects: 10,
	    ...options,
	    socketPath: undefined,
	    hostname: undefined,
	    protocol: undefined,
	    timeout: undefined,
	    method: 'GET',
	    host: undefined,
	    path: undefined,
	    port: undefined
	  };

	  websocket._autoPong = opts.autoPong;
	  websocket._closeTimeout = opts.closeTimeout;

	  if (!protocolVersions.includes(opts.protocolVersion)) {
	    throw new RangeError(
	      `Unsupported protocol version: ${opts.protocolVersion} ` +
	        `(supported versions: ${protocolVersions.join(', ')})`
	    );
	  }

	  let parsedUrl;

	  if (address instanceof URL) {
	    parsedUrl = address;
	  } else {
	    try {
	      parsedUrl = new URL(address);
	    } catch (e) {
	      throw new SyntaxError(`Invalid URL: ${address}`);
	    }
	  }

	  if (parsedUrl.protocol === 'http:') {
	    parsedUrl.protocol = 'ws:';
	  } else if (parsedUrl.protocol === 'https:') {
	    parsedUrl.protocol = 'wss:';
	  }

	  websocket._url = parsedUrl.href;

	  const isSecure = parsedUrl.protocol === 'wss:';
	  const isIpcUrl = parsedUrl.protocol === 'ws+unix:';
	  let invalidUrlMessage;

	  if (parsedUrl.protocol !== 'ws:' && !isSecure && !isIpcUrl) {
	    invalidUrlMessage =
	      'The URL\'s protocol must be one of "ws:", "wss:", ' +
	      '"http:", "https:", or "ws+unix:"';
	  } else if (isIpcUrl && !parsedUrl.pathname) {
	    invalidUrlMessage = "The URL's pathname is empty";
	  } else if (parsedUrl.hash) {
	    invalidUrlMessage = 'The URL contains a fragment identifier';
	  }

	  if (invalidUrlMessage) {
	    const err = new SyntaxError(invalidUrlMessage);

	    if (websocket._redirects === 0) {
	      throw err;
	    } else {
	      emitErrorAndClose(websocket, err);
	      return;
	    }
	  }

	  const defaultPort = isSecure ? 443 : 80;
	  const key = randomBytes(16).toString('base64');
	  const request = isSecure ? https.request : http.request;
	  const protocolSet = new Set();
	  let perMessageDeflate;

	  opts.createConnection =
	    opts.createConnection || (isSecure ? tlsConnect : netConnect);
	  opts.defaultPort = opts.defaultPort || defaultPort;
	  opts.port = parsedUrl.port || defaultPort;
	  opts.host = parsedUrl.hostname.startsWith('[')
	    ? parsedUrl.hostname.slice(1, -1)
	    : parsedUrl.hostname;
	  opts.headers = {
	    ...opts.headers,
	    'Sec-WebSocket-Version': opts.protocolVersion,
	    'Sec-WebSocket-Key': key,
	    Connection: 'Upgrade',
	    Upgrade: 'websocket'
	  };
	  opts.path = parsedUrl.pathname + parsedUrl.search;
	  opts.timeout = opts.handshakeTimeout;

	  if (opts.perMessageDeflate) {
	    perMessageDeflate = new PerMessageDeflate(
	      opts.perMessageDeflate !== true ? opts.perMessageDeflate : {},
	      false,
	      opts.maxPayload
	    );
	    opts.headers['Sec-WebSocket-Extensions'] = format({
	      [PerMessageDeflate.extensionName]: perMessageDeflate.offer()
	    });
	  }
	  if (protocols.length) {
	    for (const protocol of protocols) {
	      if (
	        typeof protocol !== 'string' ||
	        !subprotocolRegex.test(protocol) ||
	        protocolSet.has(protocol)
	      ) {
	        throw new SyntaxError(
	          'An invalid or duplicated subprotocol was specified'
	        );
	      }

	      protocolSet.add(protocol);
	    }

	    opts.headers['Sec-WebSocket-Protocol'] = protocols.join(',');
	  }
	  if (opts.origin) {
	    if (opts.protocolVersion < 13) {
	      opts.headers['Sec-WebSocket-Origin'] = opts.origin;
	    } else {
	      opts.headers.Origin = opts.origin;
	    }
	  }
	  if (parsedUrl.username || parsedUrl.password) {
	    opts.auth = `${parsedUrl.username}:${parsedUrl.password}`;
	  }

	  if (isIpcUrl) {
	    const parts = opts.path.split(':');

	    opts.socketPath = parts[0];
	    opts.path = parts[1];
	  }

	  let req;

	  if (opts.followRedirects) {
	    if (websocket._redirects === 0) {
	      websocket._originalIpc = isIpcUrl;
	      websocket._originalSecure = isSecure;
	      websocket._originalHostOrSocketPath = isIpcUrl
	        ? opts.socketPath
	        : parsedUrl.host;

	      const headers = options && options.headers;

	      //
	      // Shallow copy the user provided options so that headers can be changed
	      // without mutating the original object.
	      //
	      options = { ...options, headers: {} };

	      if (headers) {
	        for (const [key, value] of Object.entries(headers)) {
	          options.headers[key.toLowerCase()] = value;
	        }
	      }
	    } else if (websocket.listenerCount('redirect') === 0) {
	      const isSameHost = isIpcUrl
	        ? websocket._originalIpc
	          ? opts.socketPath === websocket._originalHostOrSocketPath
	          : false
	        : websocket._originalIpc
	          ? false
	          : parsedUrl.host === websocket._originalHostOrSocketPath;

	      if (!isSameHost || (websocket._originalSecure && !isSecure)) {
	        //
	        // Match curl 7.77.0 behavior and drop the following headers. These
	        // headers are also dropped when following a redirect to a subdomain.
	        //
	        delete opts.headers.authorization;
	        delete opts.headers.cookie;

	        if (!isSameHost) delete opts.headers.host;

	        opts.auth = undefined;
	      }
	    }

	    //
	    // Match curl 7.77.0 behavior and make the first `Authorization` header win.
	    // If the `Authorization` header is set, then there is nothing to do as it
	    // will take precedence.
	    //
	    if (opts.auth && !options.headers.authorization) {
	      options.headers.authorization =
	        'Basic ' + Buffer.from(opts.auth).toString('base64');
	    }

	    req = websocket._req = request(opts);

	    if (websocket._redirects) {
	      //
	      // Unlike what is done for the `'upgrade'` event, no early exit is
	      // triggered here if the user calls `websocket.close()` or
	      // `websocket.terminate()` from a listener of the `'redirect'` event. This
	      // is because the user can also call `request.destroy()` with an error
	      // before calling `websocket.close()` or `websocket.terminate()` and this
	      // would result in an error being emitted on the `request` object with no
	      // `'error'` event listeners attached.
	      //
	      websocket.emit('redirect', websocket.url, req);
	    }
	  } else {
	    req = websocket._req = request(opts);
	  }

	  if (opts.timeout) {
	    req.on('timeout', () => {
	      abortHandshake(websocket, req, 'Opening handshake has timed out');
	    });
	  }

	  req.on('error', (err) => {
	    if (req === null || req[kAborted]) return;

	    req = websocket._req = null;
	    emitErrorAndClose(websocket, err);
	  });

	  req.on('response', (res) => {
	    const location = res.headers.location;
	    const statusCode = res.statusCode;

	    if (
	      location &&
	      opts.followRedirects &&
	      statusCode >= 300 &&
	      statusCode < 400
	    ) {
	      if (++websocket._redirects > opts.maxRedirects) {
	        abortHandshake(websocket, req, 'Maximum redirects exceeded');
	        return;
	      }

	      req.abort();

	      let addr;

	      try {
	        addr = new URL(location, address);
	      } catch (e) {
	        const err = new SyntaxError(`Invalid URL: ${location}`);
	        emitErrorAndClose(websocket, err);
	        return;
	      }

	      initAsClient(websocket, addr, protocols, options);
	    } else if (!websocket.emit('unexpected-response', req, res)) {
	      abortHandshake(
	        websocket,
	        req,
	        `Unexpected server response: ${res.statusCode}`
	      );
	    }
	  });

	  req.on('upgrade', (res, socket, head) => {
	    websocket.emit('upgrade', res);

	    //
	    // The user may have closed the connection from a listener of the
	    // `'upgrade'` event.
	    //
	    if (websocket.readyState !== WebSocket.CONNECTING) return;

	    req = websocket._req = null;

	    const upgrade = res.headers.upgrade;

	    if (upgrade === undefined || upgrade.toLowerCase() !== 'websocket') {
	      abortHandshake(websocket, socket, 'Invalid Upgrade header');
	      return;
	    }

	    const digest = createHash('sha1')
	      .update(key + GUID)
	      .digest('base64');

	    if (res.headers['sec-websocket-accept'] !== digest) {
	      abortHandshake(websocket, socket, 'Invalid Sec-WebSocket-Accept header');
	      return;
	    }

	    const serverProt = res.headers['sec-websocket-protocol'];
	    let protError;

	    if (serverProt !== undefined) {
	      if (!protocolSet.size) {
	        protError = 'Server sent a subprotocol but none was requested';
	      } else if (!protocolSet.has(serverProt)) {
	        protError = 'Server sent an invalid subprotocol';
	      }
	    } else if (protocolSet.size) {
	      protError = 'Server sent no subprotocol';
	    }

	    if (protError) {
	      abortHandshake(websocket, socket, protError);
	      return;
	    }

	    if (serverProt) websocket._protocol = serverProt;

	    const secWebSocketExtensions = res.headers['sec-websocket-extensions'];

	    if (secWebSocketExtensions !== undefined) {
	      if (!perMessageDeflate) {
	        const message =
	          'Server sent a Sec-WebSocket-Extensions header but no extension ' +
	          'was requested';
	        abortHandshake(websocket, socket, message);
	        return;
	      }

	      let extensions;

	      try {
	        extensions = parse(secWebSocketExtensions);
	      } catch (err) {
	        const message = 'Invalid Sec-WebSocket-Extensions header';
	        abortHandshake(websocket, socket, message);
	        return;
	      }

	      const extensionNames = Object.keys(extensions);

	      if (
	        extensionNames.length !== 1 ||
	        extensionNames[0] !== PerMessageDeflate.extensionName
	      ) {
	        const message = 'Server indicated an extension that was not requested';
	        abortHandshake(websocket, socket, message);
	        return;
	      }

	      try {
	        perMessageDeflate.accept(extensions[PerMessageDeflate.extensionName]);
	      } catch (err) {
	        const message = 'Invalid Sec-WebSocket-Extensions header';
	        abortHandshake(websocket, socket, message);
	        return;
	      }

	      websocket._extensions[PerMessageDeflate.extensionName] =
	        perMessageDeflate;
	    }

	    websocket.setSocket(socket, head, {
	      allowSynchronousEvents: opts.allowSynchronousEvents,
	      generateMask: opts.generateMask,
	      maxPayload: opts.maxPayload,
	      skipUTF8Validation: opts.skipUTF8Validation
	    });
	  });

	  if (opts.finishRequest) {
	    opts.finishRequest(req, websocket);
	  } else {
	    req.end();
	  }
	}

	/**
	 * Emit the `'error'` and `'close'` events.
	 *
	 * @param {WebSocket} websocket The WebSocket instance
	 * @param {Error} The error to emit
	 * @private
	 */
	function emitErrorAndClose(websocket, err) {
	  websocket._readyState = WebSocket.CLOSING;
	  //
	  // The following assignment is practically useless and is done only for
	  // consistency.
	  //
	  websocket._errorEmitted = true;
	  websocket.emit('error', err);
	  websocket.emitClose();
	}

	/**
	 * Create a `net.Socket` and initiate a connection.
	 *
	 * @param {Object} options Connection options
	 * @return {net.Socket} The newly created socket used to start the connection
	 * @private
	 */
	function netConnect(options) {
	  options.path = options.socketPath;
	  return net.connect(options);
	}

	/**
	 * Create a `tls.TLSSocket` and initiate a connection.
	 *
	 * @param {Object} options Connection options
	 * @return {tls.TLSSocket} The newly created socket used to start the connection
	 * @private
	 */
	function tlsConnect(options) {
	  options.path = undefined;

	  if (!options.servername && options.servername !== '') {
	    options.servername = net.isIP(options.host) ? '' : options.host;
	  }

	  return tls.connect(options);
	}

	/**
	 * Abort the handshake and emit an error.
	 *
	 * @param {WebSocket} websocket The WebSocket instance
	 * @param {(http.ClientRequest|net.Socket|tls.Socket)} stream The request to
	 *     abort or the socket to destroy
	 * @param {String} message The error message
	 * @private
	 */
	function abortHandshake(websocket, stream, message) {
	  websocket._readyState = WebSocket.CLOSING;

	  const err = new Error(message);
	  Error.captureStackTrace(err, abortHandshake);

	  if (stream.setHeader) {
	    stream[kAborted] = true;
	    stream.abort();

	    if (stream.socket && !stream.socket.destroyed) {
	      //
	      // On Node.js >= 14.3.0 `request.abort()` does not destroy the socket if
	      // called after the request completed. See
	      // https://github.com/websockets/ws/issues/1869.
	      //
	      stream.socket.destroy();
	    }

	    process.nextTick(emitErrorAndClose, websocket, err);
	  } else {
	    stream.destroy(err);
	    stream.once('error', websocket.emit.bind(websocket, 'error'));
	    stream.once('close', websocket.emitClose.bind(websocket));
	  }
	}

	/**
	 * Handle cases where the `ping()`, `pong()`, or `send()` methods are called
	 * when the `readyState` attribute is `CLOSING` or `CLOSED`.
	 *
	 * @param {WebSocket} websocket The WebSocket instance
	 * @param {*} [data] The data to send
	 * @param {Function} [cb] Callback
	 * @private
	 */
	function sendAfterClose(websocket, data, cb) {
	  if (data) {
	    const length = isBlob(data) ? data.size : toBuffer(data).length;

	    //
	    // The `_bufferedAmount` property is used only when the peer is a client and
	    // the opening handshake fails. Under these circumstances, in fact, the
	    // `setSocket()` method is not called, so the `_socket` and `_sender`
	    // properties are set to `null`.
	    //
	    if (websocket._socket) websocket._sender._bufferedBytes += length;
	    else websocket._bufferedAmount += length;
	  }

	  if (cb) {
	    const err = new Error(
	      `WebSocket is not open: readyState ${websocket.readyState} ` +
	        `(${readyStates[websocket.readyState]})`
	    );
	    process.nextTick(cb, err);
	  }
	}

	/**
	 * The listener of the `Receiver` `'conclude'` event.
	 *
	 * @param {Number} code The status code
	 * @param {Buffer} reason The reason for closing
	 * @private
	 */
	function receiverOnConclude(code, reason) {
	  const websocket = this[kWebSocket];

	  websocket._closeFrameReceived = true;
	  websocket._closeMessage = reason;
	  websocket._closeCode = code;

	  if (websocket._socket[kWebSocket] === undefined) return;

	  websocket._socket.removeListener('data', socketOnData);
	  process.nextTick(resume, websocket._socket);

	  if (code === 1005) websocket.close();
	  else websocket.close(code, reason);
	}

	/**
	 * The listener of the `Receiver` `'drain'` event.
	 *
	 * @private
	 */
	function receiverOnDrain() {
	  const websocket = this[kWebSocket];

	  if (!websocket.isPaused) websocket._socket.resume();
	}

	/**
	 * The listener of the `Receiver` `'error'` event.
	 *
	 * @param {(RangeError|Error)} err The emitted error
	 * @private
	 */
	function receiverOnError(err) {
	  const websocket = this[kWebSocket];

	  if (websocket._socket[kWebSocket] !== undefined) {
	    websocket._socket.removeListener('data', socketOnData);

	    //
	    // On Node.js < 14.0.0 the `'error'` event is emitted synchronously. See
	    // https://github.com/websockets/ws/issues/1940.
	    //
	    process.nextTick(resume, websocket._socket);

	    websocket.close(err[kStatusCode]);
	  }

	  if (!websocket._errorEmitted) {
	    websocket._errorEmitted = true;
	    websocket.emit('error', err);
	  }
	}

	/**
	 * The listener of the `Receiver` `'finish'` event.
	 *
	 * @private
	 */
	function receiverOnFinish() {
	  this[kWebSocket].emitClose();
	}

	/**
	 * The listener of the `Receiver` `'message'` event.
	 *
	 * @param {Buffer|ArrayBuffer|Buffer[])} data The message
	 * @param {Boolean} isBinary Specifies whether the message is binary or not
	 * @private
	 */
	function receiverOnMessage(data, isBinary) {
	  this[kWebSocket].emit('message', data, isBinary);
	}

	/**
	 * The listener of the `Receiver` `'ping'` event.
	 *
	 * @param {Buffer} data The data included in the ping frame
	 * @private
	 */
	function receiverOnPing(data) {
	  const websocket = this[kWebSocket];

	  if (websocket._autoPong) websocket.pong(data, !this._isServer, NOOP);
	  websocket.emit('ping', data);
	}

	/**
	 * The listener of the `Receiver` `'pong'` event.
	 *
	 * @param {Buffer} data The data included in the pong frame
	 * @private
	 */
	function receiverOnPong(data) {
	  this[kWebSocket].emit('pong', data);
	}

	/**
	 * Resume a readable stream
	 *
	 * @param {Readable} stream The readable stream
	 * @private
	 */
	function resume(stream) {
	  stream.resume();
	}

	/**
	 * The `Sender` error event handler.
	 *
	 * @param {Error} The error
	 * @private
	 */
	function senderOnError(err) {
	  const websocket = this[kWebSocket];

	  if (websocket.readyState === WebSocket.CLOSED) return;
	  if (websocket.readyState === WebSocket.OPEN) {
	    websocket._readyState = WebSocket.CLOSING;
	    setCloseTimer(websocket);
	  }

	  //
	  // `socket.end()` is used instead of `socket.destroy()` to allow the other
	  // peer to finish sending queued data. There is no need to set a timer here
	  // because `CLOSING` means that it is already set or not needed.
	  //
	  this._socket.end();

	  if (!websocket._errorEmitted) {
	    websocket._errorEmitted = true;
	    websocket.emit('error', err);
	  }
	}

	/**
	 * Set a timer to destroy the underlying raw socket of a WebSocket.
	 *
	 * @param {WebSocket} websocket The WebSocket instance
	 * @private
	 */
	function setCloseTimer(websocket) {
	  websocket._closeTimer = setTimeout(
	    websocket._socket.destroy.bind(websocket._socket),
	    websocket._closeTimeout
	  );
	}

	/**
	 * The listener of the socket `'close'` event.
	 *
	 * @private
	 */
	function socketOnClose() {
	  const websocket = this[kWebSocket];

	  this.removeListener('close', socketOnClose);
	  this.removeListener('data', socketOnData);
	  this.removeListener('end', socketOnEnd);

	  websocket._readyState = WebSocket.CLOSING;

	  //
	  // The close frame might not have been received or the `'end'` event emitted,
	  // for example, if the socket was destroyed due to an error. Ensure that the
	  // `receiver` stream is closed after writing any remaining buffered data to
	  // it. If the readable side of the socket is in flowing mode then there is no
	  // buffered data as everything has been already written. If instead, the
	  // socket is paused, any possible buffered data will be read as a single
	  // chunk.
	  //
	  if (
	    !this._readableState.endEmitted &&
	    !websocket._closeFrameReceived &&
	    !websocket._receiver._writableState.errorEmitted &&
	    this._readableState.length !== 0
	  ) {
	    const chunk = this.read(this._readableState.length);

	    websocket._receiver.write(chunk);
	  }

	  websocket._receiver.end();

	  this[kWebSocket] = undefined;

	  clearTimeout(websocket._closeTimer);

	  if (
	    websocket._receiver._writableState.finished ||
	    websocket._receiver._writableState.errorEmitted
	  ) {
	    websocket.emitClose();
	  } else {
	    websocket._receiver.on('error', receiverOnFinish);
	    websocket._receiver.on('finish', receiverOnFinish);
	  }
	}

	/**
	 * The listener of the socket `'data'` event.
	 *
	 * @param {Buffer} chunk A chunk of data
	 * @private
	 */
	function socketOnData(chunk) {
	  if (!this[kWebSocket]._receiver.write(chunk)) {
	    this.pause();
	  }
	}

	/**
	 * The listener of the socket `'end'` event.
	 *
	 * @private
	 */
	function socketOnEnd() {
	  const websocket = this[kWebSocket];

	  websocket._readyState = WebSocket.CLOSING;
	  websocket._receiver.end();
	  this.end();
	}

	/**
	 * The listener of the socket `'error'` event.
	 *
	 * @private
	 */
	function socketOnError() {
	  const websocket = this[kWebSocket];

	  this.removeListener('error', socketOnError);
	  this.on('error', NOOP);

	  if (websocket) {
	    websocket._readyState = WebSocket.CLOSING;
	    this.destroy();
	  }
	}
	return websocket;
}

/* eslint no-unused-vars: ["error", { "varsIgnorePattern": "^WebSocket$" }] */

var stream;
var hasRequiredStream;

function requireStream () {
	if (hasRequiredStream) return stream;
	hasRequiredStream = 1;

	requireWebsocket();
	const { Duplex } = require$$0$2;

	/**
	 * Emits the `'close'` event on a stream.
	 *
	 * @param {Duplex} stream The stream.
	 * @private
	 */
	function emitClose(stream) {
	  stream.emit('close');
	}

	/**
	 * The listener of the `'end'` event.
	 *
	 * @private
	 */
	function duplexOnEnd() {
	  if (!this.destroyed && this._writableState.finished) {
	    this.destroy();
	  }
	}

	/**
	 * The listener of the `'error'` event.
	 *
	 * @param {Error} err The error
	 * @private
	 */
	function duplexOnError(err) {
	  this.removeListener('error', duplexOnError);
	  this.destroy();
	  if (this.listenerCount('error') === 0) {
	    // Do not suppress the throwing behavior.
	    this.emit('error', err);
	  }
	}

	/**
	 * Wraps a `WebSocket` in a duplex stream.
	 *
	 * @param {WebSocket} ws The `WebSocket` to wrap
	 * @param {Object} [options] The options for the `Duplex` constructor
	 * @return {Duplex} The duplex stream
	 * @public
	 */
	function createWebSocketStream(ws, options) {
	  let terminateOnDestroy = true;

	  const duplex = new Duplex({
	    ...options,
	    autoDestroy: false,
	    emitClose: false,
	    objectMode: false,
	    writableObjectMode: false
	  });

	  ws.on('message', function message(msg, isBinary) {
	    const data =
	      !isBinary && duplex._readableState.objectMode ? msg.toString() : msg;

	    if (!duplex.push(data)) ws.pause();
	  });

	  ws.once('error', function error(err) {
	    if (duplex.destroyed) return;

	    // Prevent `ws.terminate()` from being called by `duplex._destroy()`.
	    //
	    // - If the `'error'` event is emitted before the `'open'` event, then
	    //   `ws.terminate()` is a noop as no socket is assigned.
	    // - Otherwise, the error is re-emitted by the listener of the `'error'`
	    //   event of the `Receiver` object. The listener already closes the
	    //   connection by calling `ws.close()`. This allows a close frame to be
	    //   sent to the other peer. If `ws.terminate()` is called right after this,
	    //   then the close frame might not be sent.
	    terminateOnDestroy = false;
	    duplex.destroy(err);
	  });

	  ws.once('close', function close() {
	    if (duplex.destroyed) return;

	    duplex.push(null);
	  });

	  duplex._destroy = function (err, callback) {
	    if (ws.readyState === ws.CLOSED) {
	      callback(err);
	      process.nextTick(emitClose, duplex);
	      return;
	    }

	    let called = false;

	    ws.once('error', function error(err) {
	      called = true;
	      callback(err);
	    });

	    ws.once('close', function close() {
	      if (!called) callback(err);
	      process.nextTick(emitClose, duplex);
	    });

	    if (terminateOnDestroy) ws.terminate();
	  };

	  duplex._final = function (callback) {
	    if (ws.readyState === ws.CONNECTING) {
	      ws.once('open', function open() {
	        duplex._final(callback);
	      });
	      return;
	    }

	    // If the value of the `_socket` property is `null` it means that `ws` is a
	    // client websocket and the handshake failed. In fact, when this happens, a
	    // socket is never assigned to the websocket. Wait for the `'error'` event
	    // that will be emitted by the websocket.
	    if (ws._socket === null) return;

	    if (ws._socket._writableState.finished) {
	      callback();
	      if (duplex._readableState.endEmitted) duplex.destroy();
	    } else {
	      ws._socket.once('finish', function finish() {
	        // `duplex` is not destroyed here because the `'end'` event will be
	        // emitted on `duplex` after this `'finish'` event. The EOF signaling
	        // `null` chunk is, in fact, pushed when the websocket emits `'close'`.
	        callback();
	      });
	      ws.close();
	    }
	  };

	  duplex._read = function () {
	    if (ws.isPaused) ws.resume();
	  };

	  duplex._write = function (chunk, encoding, callback) {
	    if (ws.readyState === ws.CONNECTING) {
	      ws.once('open', function open() {
	        duplex._write(chunk, encoding, callback);
	      });
	      return;
	    }

	    ws.send(chunk, callback);
	  };

	  duplex.on('end', duplexOnEnd);
	  duplex.on('error', duplexOnError);
	  return duplex;
	}

	stream = createWebSocketStream;
	return stream;
}

requireStream();

requireReceiver();

requireSender();

var websocketExports = requireWebsocket();
var WebSocket = /*@__PURE__*/getDefaultExportFromCjs(websocketExports);

var subprotocol;
var hasRequiredSubprotocol;

function requireSubprotocol () {
	if (hasRequiredSubprotocol) return subprotocol;
	hasRequiredSubprotocol = 1;

	const { tokenChars } = requireValidation();

	/**
	 * Parses the `Sec-WebSocket-Protocol` header into a set of subprotocol names.
	 *
	 * @param {String} header The field value of the header
	 * @return {Set} The subprotocol names
	 * @public
	 */
	function parse(header) {
	  const protocols = new Set();
	  let start = -1;
	  let end = -1;
	  let i = 0;

	  for (i; i < header.length; i++) {
	    const code = header.charCodeAt(i);

	    if (end === -1 && tokenChars[code] === 1) {
	      if (start === -1) start = i;
	    } else if (
	      i !== 0 &&
	      (code === 0x20 /* ' ' */ || code === 0x09) /* '\t' */
	    ) {
	      if (end === -1 && start !== -1) end = i;
	    } else if (code === 0x2c /* ',' */) {
	      if (start === -1) {
	        throw new SyntaxError(`Unexpected character at index ${i}`);
	      }

	      if (end === -1) end = i;

	      const protocol = header.slice(start, end);

	      if (protocols.has(protocol)) {
	        throw new SyntaxError(`The "${protocol}" subprotocol is duplicated`);
	      }

	      protocols.add(protocol);
	      start = end = -1;
	    } else {
	      throw new SyntaxError(`Unexpected character at index ${i}`);
	    }
	  }

	  if (start === -1 || end !== -1) {
	    throw new SyntaxError('Unexpected end of input');
	  }

	  const protocol = header.slice(start, i);

	  if (protocols.has(protocol)) {
	    throw new SyntaxError(`The "${protocol}" subprotocol is duplicated`);
	  }

	  protocols.add(protocol);
	  return protocols;
	}

	subprotocol = { parse };
	return subprotocol;
}

/* eslint no-unused-vars: ["error", { "varsIgnorePattern": "^Duplex$", "caughtErrors": "none" }] */

var websocketServer;
var hasRequiredWebsocketServer;

function requireWebsocketServer () {
	if (hasRequiredWebsocketServer) return websocketServer;
	hasRequiredWebsocketServer = 1;

	const EventEmitter = require$$0$3;
	const http = require$$2;
	const { Duplex } = require$$0$2;
	const { createHash } = require$$1;

	const extension = requireExtension();
	const PerMessageDeflate = requirePermessageDeflate();
	const subprotocol = requireSubprotocol();
	const WebSocket = requireWebsocket();
	const { CLOSE_TIMEOUT, GUID, kWebSocket } = requireConstants();

	const keyRegex = /^[+/0-9A-Za-z]{22}==$/;

	const RUNNING = 0;
	const CLOSING = 1;
	const CLOSED = 2;

	/**
	 * Class representing a WebSocket server.
	 *
	 * @extends EventEmitter
	 */
	class WebSocketServer extends EventEmitter {
	  /**
	   * Create a `WebSocketServer` instance.
	   *
	   * @param {Object} options Configuration options
	   * @param {Boolean} [options.allowSynchronousEvents=true] Specifies whether
	   *     any of the `'message'`, `'ping'`, and `'pong'` events can be emitted
	   *     multiple times in the same tick
	   * @param {Boolean} [options.autoPong=true] Specifies whether or not to
	   *     automatically send a pong in response to a ping
	   * @param {Number} [options.backlog=511] The maximum length of the queue of
	   *     pending connections
	   * @param {Boolean} [options.clientTracking=true] Specifies whether or not to
	   *     track clients
	   * @param {Number} [options.closeTimeout=30000] Duration in milliseconds to
	   *     wait for the closing handshake to finish after `websocket.close()` is
	   *     called
	   * @param {Function} [options.handleProtocols] A hook to handle protocols
	   * @param {String} [options.host] The hostname where to bind the server
	   * @param {Number} [options.maxPayload=104857600] The maximum allowed message
	   *     size
	   * @param {Boolean} [options.noServer=false] Enable no server mode
	   * @param {String} [options.path] Accept only connections matching this path
	   * @param {(Boolean|Object)} [options.perMessageDeflate=false] Enable/disable
	   *     permessage-deflate
	   * @param {Number} [options.port] The port where to bind the server
	   * @param {(http.Server|https.Server)} [options.server] A pre-created HTTP/S
	   *     server to use
	   * @param {Boolean} [options.skipUTF8Validation=false] Specifies whether or
	   *     not to skip UTF-8 validation for text and close messages
	   * @param {Function} [options.verifyClient] A hook to reject connections
	   * @param {Function} [options.WebSocket=WebSocket] Specifies the `WebSocket`
	   *     class to use. It must be the `WebSocket` class or class that extends it
	   * @param {Function} [callback] A listener for the `listening` event
	   */
	  constructor(options, callback) {
	    super();

	    options = {
	      allowSynchronousEvents: true,
	      autoPong: true,
	      maxPayload: 100 * 1024 * 1024,
	      skipUTF8Validation: false,
	      perMessageDeflate: false,
	      handleProtocols: null,
	      clientTracking: true,
	      closeTimeout: CLOSE_TIMEOUT,
	      verifyClient: null,
	      noServer: false,
	      backlog: null, // use default (511 as implemented in net.js)
	      server: null,
	      host: null,
	      path: null,
	      port: null,
	      WebSocket,
	      ...options
	    };

	    if (
	      (options.port == null && !options.server && !options.noServer) ||
	      (options.port != null && (options.server || options.noServer)) ||
	      (options.server && options.noServer)
	    ) {
	      throw new TypeError(
	        'One and only one of the "port", "server", or "noServer" options ' +
	          'must be specified'
	      );
	    }

	    if (options.port != null) {
	      this._server = http.createServer((req, res) => {
	        const body = http.STATUS_CODES[426];

	        res.writeHead(426, {
	          'Content-Length': body.length,
	          'Content-Type': 'text/plain'
	        });
	        res.end(body);
	      });
	      this._server.listen(
	        options.port,
	        options.host,
	        options.backlog,
	        callback
	      );
	    } else if (options.server) {
	      this._server = options.server;
	    }

	    if (this._server) {
	      const emitConnection = this.emit.bind(this, 'connection');

	      this._removeListeners = addListeners(this._server, {
	        listening: this.emit.bind(this, 'listening'),
	        error: this.emit.bind(this, 'error'),
	        upgrade: (req, socket, head) => {
	          this.handleUpgrade(req, socket, head, emitConnection);
	        }
	      });
	    }

	    if (options.perMessageDeflate === true) options.perMessageDeflate = {};
	    if (options.clientTracking) {
	      this.clients = new Set();
	      this._shouldEmitClose = false;
	    }

	    this.options = options;
	    this._state = RUNNING;
	  }

	  /**
	   * Returns the bound address, the address family name, and port of the server
	   * as reported by the operating system if listening on an IP socket.
	   * If the server is listening on a pipe or UNIX domain socket, the name is
	   * returned as a string.
	   *
	   * @return {(Object|String|null)} The address of the server
	   * @public
	   */
	  address() {
	    if (this.options.noServer) {
	      throw new Error('The server is operating in "noServer" mode');
	    }

	    if (!this._server) return null;
	    return this._server.address();
	  }

	  /**
	   * Stop the server from accepting new connections and emit the `'close'` event
	   * when all existing connections are closed.
	   *
	   * @param {Function} [cb] A one-time listener for the `'close'` event
	   * @public
	   */
	  close(cb) {
	    if (this._state === CLOSED) {
	      if (cb) {
	        this.once('close', () => {
	          cb(new Error('The server is not running'));
	        });
	      }

	      process.nextTick(emitClose, this);
	      return;
	    }

	    if (cb) this.once('close', cb);

	    if (this._state === CLOSING) return;
	    this._state = CLOSING;

	    if (this.options.noServer || this.options.server) {
	      if (this._server) {
	        this._removeListeners();
	        this._removeListeners = this._server = null;
	      }

	      if (this.clients) {
	        if (!this.clients.size) {
	          process.nextTick(emitClose, this);
	        } else {
	          this._shouldEmitClose = true;
	        }
	      } else {
	        process.nextTick(emitClose, this);
	      }
	    } else {
	      const server = this._server;

	      this._removeListeners();
	      this._removeListeners = this._server = null;

	      //
	      // The HTTP/S server was created internally. Close it, and rely on its
	      // `'close'` event.
	      //
	      server.close(() => {
	        emitClose(this);
	      });
	    }
	  }

	  /**
	   * See if a given request should be handled by this server instance.
	   *
	   * @param {http.IncomingMessage} req Request object to inspect
	   * @return {Boolean} `true` if the request is valid, else `false`
	   * @public
	   */
	  shouldHandle(req) {
	    if (this.options.path) {
	      const index = req.url.indexOf('?');
	      const pathname = index !== -1 ? req.url.slice(0, index) : req.url;

	      if (pathname !== this.options.path) return false;
	    }

	    return true;
	  }

	  /**
	   * Handle a HTTP Upgrade request.
	   *
	   * @param {http.IncomingMessage} req The request object
	   * @param {Duplex} socket The network socket between the server and client
	   * @param {Buffer} head The first packet of the upgraded stream
	   * @param {Function} cb Callback
	   * @public
	   */
	  handleUpgrade(req, socket, head, cb) {
	    socket.on('error', socketOnError);

	    const key = req.headers['sec-websocket-key'];
	    const upgrade = req.headers.upgrade;
	    const version = +req.headers['sec-websocket-version'];

	    if (req.method !== 'GET') {
	      const message = 'Invalid HTTP method';
	      abortHandshakeOrEmitwsClientError(this, req, socket, 405, message);
	      return;
	    }

	    if (upgrade === undefined || upgrade.toLowerCase() !== 'websocket') {
	      const message = 'Invalid Upgrade header';
	      abortHandshakeOrEmitwsClientError(this, req, socket, 400, message);
	      return;
	    }

	    if (key === undefined || !keyRegex.test(key)) {
	      const message = 'Missing or invalid Sec-WebSocket-Key header';
	      abortHandshakeOrEmitwsClientError(this, req, socket, 400, message);
	      return;
	    }

	    if (version !== 13 && version !== 8) {
	      const message = 'Missing or invalid Sec-WebSocket-Version header';
	      abortHandshakeOrEmitwsClientError(this, req, socket, 400, message, {
	        'Sec-WebSocket-Version': '13, 8'
	      });
	      return;
	    }

	    if (!this.shouldHandle(req)) {
	      abortHandshake(socket, 400);
	      return;
	    }

	    const secWebSocketProtocol = req.headers['sec-websocket-protocol'];
	    let protocols = new Set();

	    if (secWebSocketProtocol !== undefined) {
	      try {
	        protocols = subprotocol.parse(secWebSocketProtocol);
	      } catch (err) {
	        const message = 'Invalid Sec-WebSocket-Protocol header';
	        abortHandshakeOrEmitwsClientError(this, req, socket, 400, message);
	        return;
	      }
	    }

	    const secWebSocketExtensions = req.headers['sec-websocket-extensions'];
	    const extensions = {};

	    if (
	      this.options.perMessageDeflate &&
	      secWebSocketExtensions !== undefined
	    ) {
	      const perMessageDeflate = new PerMessageDeflate(
	        this.options.perMessageDeflate,
	        true,
	        this.options.maxPayload
	      );

	      try {
	        const offers = extension.parse(secWebSocketExtensions);

	        if (offers[PerMessageDeflate.extensionName]) {
	          perMessageDeflate.accept(offers[PerMessageDeflate.extensionName]);
	          extensions[PerMessageDeflate.extensionName] = perMessageDeflate;
	        }
	      } catch (err) {
	        const message =
	          'Invalid or unacceptable Sec-WebSocket-Extensions header';
	        abortHandshakeOrEmitwsClientError(this, req, socket, 400, message);
	        return;
	      }
	    }

	    //
	    // Optionally call external client verification handler.
	    //
	    if (this.options.verifyClient) {
	      const info = {
	        origin:
	          req.headers[`${version === 8 ? 'sec-websocket-origin' : 'origin'}`],
	        secure: !!(req.socket.authorized || req.socket.encrypted),
	        req
	      };

	      if (this.options.verifyClient.length === 2) {
	        this.options.verifyClient(info, (verified, code, message, headers) => {
	          if (!verified) {
	            return abortHandshake(socket, code || 401, message, headers);
	          }

	          this.completeUpgrade(
	            extensions,
	            key,
	            protocols,
	            req,
	            socket,
	            head,
	            cb
	          );
	        });
	        return;
	      }

	      if (!this.options.verifyClient(info)) return abortHandshake(socket, 401);
	    }

	    this.completeUpgrade(extensions, key, protocols, req, socket, head, cb);
	  }

	  /**
	   * Upgrade the connection to WebSocket.
	   *
	   * @param {Object} extensions The accepted extensions
	   * @param {String} key The value of the `Sec-WebSocket-Key` header
	   * @param {Set} protocols The subprotocols
	   * @param {http.IncomingMessage} req The request object
	   * @param {Duplex} socket The network socket between the server and client
	   * @param {Buffer} head The first packet of the upgraded stream
	   * @param {Function} cb Callback
	   * @throws {Error} If called more than once with the same socket
	   * @private
	   */
	  completeUpgrade(extensions, key, protocols, req, socket, head, cb) {
	    //
	    // Destroy the socket if the client has already sent a FIN packet.
	    //
	    if (!socket.readable || !socket.writable) return socket.destroy();

	    if (socket[kWebSocket]) {
	      throw new Error(
	        'server.handleUpgrade() was called more than once with the same ' +
	          'socket, possibly due to a misconfiguration'
	      );
	    }

	    if (this._state > RUNNING) return abortHandshake(socket, 503);

	    const digest = createHash('sha1')
	      .update(key + GUID)
	      .digest('base64');

	    const headers = [
	      'HTTP/1.1 101 Switching Protocols',
	      'Upgrade: websocket',
	      'Connection: Upgrade',
	      `Sec-WebSocket-Accept: ${digest}`
	    ];

	    const ws = new this.options.WebSocket(null, undefined, this.options);

	    if (protocols.size) {
	      //
	      // Optionally call external protocol selection handler.
	      //
	      const protocol = this.options.handleProtocols
	        ? this.options.handleProtocols(protocols, req)
	        : protocols.values().next().value;

	      if (protocol) {
	        headers.push(`Sec-WebSocket-Protocol: ${protocol}`);
	        ws._protocol = protocol;
	      }
	    }

	    if (extensions[PerMessageDeflate.extensionName]) {
	      const params = extensions[PerMessageDeflate.extensionName].params;
	      const value = extension.format({
	        [PerMessageDeflate.extensionName]: [params]
	      });
	      headers.push(`Sec-WebSocket-Extensions: ${value}`);
	      ws._extensions = extensions;
	    }

	    //
	    // Allow external modification/inspection of handshake headers.
	    //
	    this.emit('headers', headers, req);

	    socket.write(headers.concat('\r\n').join('\r\n'));
	    socket.removeListener('error', socketOnError);

	    ws.setSocket(socket, head, {
	      allowSynchronousEvents: this.options.allowSynchronousEvents,
	      maxPayload: this.options.maxPayload,
	      skipUTF8Validation: this.options.skipUTF8Validation
	    });

	    if (this.clients) {
	      this.clients.add(ws);
	      ws.on('close', () => {
	        this.clients.delete(ws);

	        if (this._shouldEmitClose && !this.clients.size) {
	          process.nextTick(emitClose, this);
	        }
	      });
	    }

	    cb(ws, req);
	  }
	}

	websocketServer = WebSocketServer;

	/**
	 * Add event listeners on an `EventEmitter` using a map of <event, listener>
	 * pairs.
	 *
	 * @param {EventEmitter} server The event emitter
	 * @param {Object.<String, Function>} map The listeners to add
	 * @return {Function} A function that will remove the added listeners when
	 *     called
	 * @private
	 */
	function addListeners(server, map) {
	  for (const event of Object.keys(map)) server.on(event, map[event]);

	  return function removeListeners() {
	    for (const event of Object.keys(map)) {
	      server.removeListener(event, map[event]);
	    }
	  };
	}

	/**
	 * Emit a `'close'` event on an `EventEmitter`.
	 *
	 * @param {EventEmitter} server The event emitter
	 * @private
	 */
	function emitClose(server) {
	  server._state = CLOSED;
	  server.emit('close');
	}

	/**
	 * Handle socket errors.
	 *
	 * @private
	 */
	function socketOnError() {
	  this.destroy();
	}

	/**
	 * Close the connection when preconditions are not fulfilled.
	 *
	 * @param {Duplex} socket The socket of the upgrade request
	 * @param {Number} code The HTTP response status code
	 * @param {String} [message] The HTTP response body
	 * @param {Object} [headers] Additional HTTP response headers
	 * @private
	 */
	function abortHandshake(socket, code, message, headers) {
	  //
	  // The socket is writable unless the user destroyed or ended it before calling
	  // `server.handleUpgrade()` or in the `verifyClient` function, which is a user
	  // error. Handling this does not make much sense as the worst that can happen
	  // is that some of the data written by the user might be discarded due to the
	  // call to `socket.end()` below, which triggers an `'error'` event that in
	  // turn causes the socket to be destroyed.
	  //
	  message = message || http.STATUS_CODES[code];
	  headers = {
	    Connection: 'close',
	    'Content-Type': 'text/html',
	    'Content-Length': Buffer.byteLength(message),
	    ...headers
	  };

	  socket.once('finish', socket.destroy);

	  socket.end(
	    `HTTP/1.1 ${code} ${http.STATUS_CODES[code]}\r\n` +
	      Object.keys(headers)
	        .map((h) => `${h}: ${headers[h]}`)
	        .join('\r\n') +
	      '\r\n\r\n' +
	      message
	  );
	}

	/**
	 * Emit a `'wsClientError'` event on a `WebSocketServer` if there is at least
	 * one listener for it, otherwise call `abortHandshake()`.
	 *
	 * @param {WebSocketServer} server The WebSocket server
	 * @param {http.IncomingMessage} req The request object
	 * @param {Duplex} socket The socket of the upgrade request
	 * @param {Number} code The HTTP response status code
	 * @param {String} message The HTTP response body
	 * @param {Object} [headers] The HTTP response headers
	 * @private
	 */
	function abortHandshakeOrEmitwsClientError(
	  server,
	  req,
	  socket,
	  code,
	  message,
	  headers
	) {
	  if (server.listenerCount('wsClientError')) {
	    const err = new Error(message);
	    Error.captureStackTrace(err, abortHandshakeOrEmitwsClientError);

	    server.emit('wsClientError', err, socket, req);
	  } else {
	    abortHandshake(socket, code, message, headers);
	  }
	}
	return websocketServer;
}

var websocketServerExports = requireWebsocketServer();
var WebSocketServer = /*@__PURE__*/getDefaultExportFromCjs(websocketServerExports);

/**!
 * @author Elgato
 * @module elgato/streamdeck
 * @license MIT
 * @copyright Copyright (c) Corsair Memory Inc.
 */
/**
 * Stream Deck device types.
 */
var DeviceType;
(function (DeviceType) {
    /**
     * Stream Deck, comprised of 15 customizable LCD keys in a 5 x 3 layout.
     */
    DeviceType[DeviceType["StreamDeck"] = 0] = "StreamDeck";
    /**
     * Stream Deck Mini, comprised of 6 customizable LCD keys in a 3 x 2 layout.
     */
    DeviceType[DeviceType["StreamDeckMini"] = 1] = "StreamDeckMini";
    /**
     * Stream Deck XL, comprised of 32 customizable LCD keys in an 8 x 4 layout.
     */
    DeviceType[DeviceType["StreamDeckXL"] = 2] = "StreamDeckXL";
    /**
     * Stream Deck Mobile, for iOS and Android.
     */
    DeviceType[DeviceType["StreamDeckMobile"] = 3] = "StreamDeckMobile";
    /**
     * Corsair G Keys, available on select Corsair keyboards.
     */
    DeviceType[DeviceType["CorsairGKeys"] = 4] = "CorsairGKeys";
    /**
     * Stream Deck Pedal, comprised of 3 customizable pedals.
     */
    DeviceType[DeviceType["StreamDeckPedal"] = 5] = "StreamDeckPedal";
    /**
     * Corsair Voyager laptop, comprising 10 buttons in a horizontal line above the keyboard.
     */
    DeviceType[DeviceType["CorsairVoyager"] = 6] = "CorsairVoyager";
    /**
     * Stream Deck +, comprised of 8 customizable LCD keys in a 4 x 2 layout, a touch strip, and 4 dials.
     */
    DeviceType[DeviceType["StreamDeckPlus"] = 7] = "StreamDeckPlus";
    /**
     * SCUF controller G keys, available on select SCUF controllers, for example SCUF Envision.
     */
    DeviceType[DeviceType["SCUFController"] = 8] = "SCUFController";
    /**
     * Stream Deck Neo, comprised of 8 customizable LCD keys in a 4 x 2 layout, an info bar, and 2 touch points for page navigation.
     */
    DeviceType[DeviceType["StreamDeckNeo"] = 9] = "StreamDeckNeo";
    /**
     * Stream Deck Studio, comprised of 32 customizable LCD keys in a 16 x 2 layout, and 2 dials (1 on either side).
     */
    DeviceType[DeviceType["StreamDeckStudio"] = 10] = "StreamDeckStudio";
    /**
     * Virtual Stream Deck, comprised of 1 to 64 action (on-screen) on a scalable canvas, with a maximum layout of 8 x 8.
     */
    DeviceType[DeviceType["VirtualStreamDeck"] = 11] = "VirtualStreamDeck";
})(DeviceType || (DeviceType = {}));

/**
 * List of available types that can be applied to {@link Bar} and {@link GBar} to determine their style.
 */
var BarSubType;
(function (BarSubType) {
    /**
     * Rectangle bar; the bar fills from left to right, determined by the {@link Bar.value}, similar to a standard progress bar.
     */
    BarSubType[BarSubType["Rectangle"] = 0] = "Rectangle";
    /**
     * Rectangle bar; the bar fills outwards from the centre of the bar, determined by the {@link Bar.value}.
     * @example
     * // Value is 2, range is 1-10.
     * // [  ███     ]
     * @example
     * // Value is 10, range is 1-10.
     * // [     █████]
     */
    BarSubType[BarSubType["DoubleRectangle"] = 1] = "DoubleRectangle";
    /**
     * Trapezoid bar, represented as a right-angle triangle; the bar fills from left to right, determined by the {@link Bar.value}, similar to a volume meter.
     */
    BarSubType[BarSubType["Trapezoid"] = 2] = "Trapezoid";
    /**
     * Trapezoid bar, represented by two right-angle triangles; the bar fills outwards from the centre of the bar, determined by the {@link Bar.value}. See {@link BarSubType.DoubleRectangle}.
     */
    BarSubType[BarSubType["DoubleTrapezoid"] = 3] = "DoubleTrapezoid";
    /**
     * Rounded rectangle bar; the bar fills from left to right, determined by the {@link Bar.value}, similar to a standard progress bar.
     */
    BarSubType[BarSubType["Groove"] = 4] = "Groove";
})(BarSubType || (BarSubType = {}));

/**
 * Defines the type of argument supplied by Stream Deck.
 */
var RegistrationParameter;
(function (RegistrationParameter) {
    /**
     * Identifies the argument that specifies the web socket port that Stream Deck is listening on.
     */
    RegistrationParameter["Port"] = "-port";
    /**
     * Identifies the argument that supplies information about the Stream Deck and the plugin.
     */
    RegistrationParameter["Info"] = "-info";
    /**
     * Identifies the argument that specifies the unique identifier that can be used when registering the plugin.
     */
    RegistrationParameter["PluginUUID"] = "-pluginUUID";
    /**
     * Identifies the argument that specifies the event to be sent to Stream Deck as part of the registration procedure.
     */
    RegistrationParameter["RegisterEvent"] = "-registerEvent";
})(RegistrationParameter || (RegistrationParameter = {}));

/**
 * Defines the target of a request, i.e. whether the request should update the Stream Deck hardware, Stream Deck software (application), or both, when calling `setImage` and `setState`.
 */
var Target;
(function (Target) {
    /**
     * Hardware and software should be updated as part of the request.
     */
    Target[Target["HardwareAndSoftware"] = 0] = "HardwareAndSoftware";
    /**
     * Hardware only should be updated as part of the request.
     */
    Target[Target["Hardware"] = 1] = "Hardware";
    /**
     * Software only should be updated as part of the request.
     */
    Target[Target["Software"] = 2] = "Software";
})(Target || (Target = {}));

/**
 * Provides information for a version, as parsed from a string denoted as a collection of numbers separated by a period, for example `1.45.2`, `4.0.2.13098`. Parsing is opinionated
 * and strings should strictly conform to the format `{major}[.{minor}[.{patch}[.{build}]]]`; version numbers that form the version are optional, and when `undefined` will default to
 * 0, for example the `minor`, `patch`, or `build` number may be omitted.
 *
 * NB: This implementation should be considered fit-for-purpose, and should be used sparing.
 */
class Version {
    /**
     * Build version number.
     */
    build;
    /**
     * Major version number.
     */
    major;
    /**
     * Minor version number.
     */
    minor;
    /**
     * Patch version number.
     */
    patch;
    /**
     * Initializes a new instance of the {@link Version} class.
     * @param value Value to parse the version from.
     */
    constructor(value) {
        const result = value.match(/^(0|[1-9]\d*)(?:\.(0|[1-9]\d*))?(?:\.(0|[1-9]\d*))?(?:\.(0|[1-9]\d*))?$/);
        if (result === null) {
            throw new Error(`Invalid format; expected "{major}[.{minor}[.{patch}[.{build}]]]" but was "${value}"`);
        }
        [, this.major, this.minor, this.patch, this.build] = [...result.map((value) => parseInt(value) || 0)];
    }
    /**
     * Compares this instance to the {@link other} {@link Version}.
     * @param other The {@link Version} to compare to.
     * @returns `-1` when this instance is less than the {@link other}, `1` when this instance is greater than {@link other}, otherwise `0`.
     */
    compareTo(other) {
        const segments = ({ major, minor, build, patch }) => [major, minor, build, patch];
        const thisSegments = segments(this);
        const otherSegments = segments(other);
        for (let i = 0; i < 4; i++) {
            if (thisSegments[i] < otherSegments[i]) {
                return -1;
            }
            else if (thisSegments[i] > otherSegments[i]) {
                return 1;
            }
        }
        return 0;
    }
    /** @inheritdoc */
    toString() {
        return `${this.major}.${this.minor}`;
    }
}

/**
 * Provides a {@link LogTarget} that logs to the console.
 */
class ConsoleTarget {
    /**
     * @inheritdoc
     */
    write(entry) {
        switch (entry.level) {
            case "error":
                console.error(...entry.data);
                break;
            case "warn":
                console.warn(...entry.data);
                break;
            default:
                console.log(...entry.data);
        }
    }
}

// Remove any dependencies on node.
const EOL = "\n";
/**
 * Creates a new string log entry formatter.
 * @param opts Options that defines the type for the formatter.
 * @returns The string {@link LogEntryFormatter}.
 */
function stringFormatter(opts) {
    {
        return (entry) => {
            const { data, level, scope } = entry;
            let prefix = `${new Date().toISOString()} ${level.toUpperCase().padEnd(5)} `;
            if (scope) {
                prefix += `${scope}: `;
            }
            return `${prefix}${reduce(data)}`;
        };
    }
}
/**
 * Stringifies the provided data parameters that make up the log entry.
 * @param data Data parameters.
 * @returns The data represented as a single `string`.
 */
function reduce(data) {
    let result = "";
    let previousWasError = false;
    for (const value of data) {
        // When the value is an error, write the stack.
        if (typeof value === "object" && value instanceof Error) {
            result += `${EOL}${value.stack}`;
            previousWasError = true;
            continue;
        }
        // When the previous was an error, write a new line.
        if (previousWasError) {
            result += EOL;
            previousWasError = false;
        }
        result += typeof value === "object" ? JSON.stringify(value) : value;
        result += " ";
    }
    return result.trimEnd();
}

/* eslint-disable @typescript-eslint/sort-type-constituents */
/**
 * Gets the priority of the specified log level as a number; low numbers signify a higher priority.
 * @param level Log level.
 * @returns The priority as a number.
 */
function defcon(level) {
    switch (level) {
        case "error":
            return 0;
        case "warn":
            return 1;
        case "info":
            return 2;
        case "debug":
            return 3;
        case "trace":
        default:
            return 4;
    }
}

/**
 * Logger capable of forwarding messages to a {@link LogTarget}.
 */
class Logger {
    /**
     * Backing field for the {@link Logger.level}.
     */
    #level;
    /**
     * Options that define the loggers behavior.
     */
    #options;
    /**
     * Scope associated with this {@link Logger}.
     */
    #scope;
    /**
     * Initializes a new instance of the {@link Logger} class.
     * @param opts Options that define the loggers behavior.
     */
    constructor(opts) {
        this.#options = { minimumLevel: "trace", ...opts };
        this.#scope = this.#options.scope === undefined || this.#options.scope.trim() === "" ? "" : this.#options.scope;
        if (typeof this.#options.level !== "function") {
            this.setLevel(this.#options.level);
        }
    }
    /**
     * Gets the {@link LogLevel}.
     * @returns The {@link LogLevel}.
     */
    get level() {
        if (this.#level !== undefined) {
            return this.#level;
        }
        return typeof this.#options.level === "function" ? this.#options.level() : this.#options.level;
    }
    /**
     * Creates a scoped logger with the given {@link scope}; logs created by scoped-loggers include their scope to enable their source to be easily identified.
     * @param scope Value that represents the scope of the new logger.
     * @returns The scoped logger, or this instance when {@link scope} is not defined.
     */
    createScope(scope) {
        scope = scope.trim();
        if (scope === "") {
            return this;
        }
        return new Logger({
            ...this.#options,
            level: () => this.level,
            scope: this.#options.scope ? `${this.#options.scope}->${scope}` : scope,
        });
    }
    /**
     * Writes the arguments as a debug log entry.
     * @param data Message or data to log.
     * @returns This instance for chaining.
     */
    debug(...data) {
        return this.write({ level: "debug", data, scope: this.#scope });
    }
    /**
     * Writes the arguments as error log entry.
     * @param data Message or data to log.
     * @returns This instance for chaining.
     */
    error(...data) {
        return this.write({ level: "error", data, scope: this.#scope });
    }
    /**
     * Writes the arguments as an info log entry.
     * @param data Message or data to log.
     * @returns This instance for chaining.
     */
    info(...data) {
        return this.write({ level: "info", data, scope: this.#scope });
    }
    /**
     * Sets the log-level that determines which logs should be written. The specified level will be inherited by all scoped loggers unless they have log-level explicitly defined.
     * @param level The log-level that determines which logs should be written; when `undefined`, the level will be inherited from the parent logger, or default to the environment level.
     * @returns This instance for chaining.
     */
    setLevel(level) {
        if (level !== undefined && defcon(level) > defcon(this.#options.minimumLevel)) {
            this.#level = "info";
        }
        else {
            this.#level = level;
        }
        return this;
    }
    /**
     * Writes the arguments as a trace log entry.
     * @param data Message or data to log.
     * @returns This instance for chaining.
     */
    trace(...data) {
        return this.write({ level: "trace", data, scope: this.#scope });
    }
    /**
     * Writes the arguments as a warning log entry.
     * @param data Message or data to log.
     * @returns This instance for chaining.
     */
    warn(...data) {
        return this.write({ level: "warn", data, scope: this.#scope });
    }
    /**
     * Writes the log entry.
     * @param entry Log entry to write.
     * @returns This instance for chaining.
     */
    write(entry) {
        if (defcon(entry.level) <= defcon(this.level)) {
            this.#options.targets.forEach((t) => t.write(entry));
        }
        return this;
    }
}

/**
 * Provides a {@link LogTarget} capable of logging to a local file system.
 */
class FileTarget {
    /**
     * File path where logs will be written.
     */
    #filePath;
    /**
     * Options that defines how logs should be written to the local file system.
     */
    #options;
    /**
     * Current size of the logs that have been written to the {@link FileTarget.#filePath}.
     */
    #size = 0;
    /**
     * Initializes a new instance of the {@link FileTarget} class.
     * @param options Options that defines how logs should be written to the local file system.
     */
    constructor(options) {
        this.#options = options;
        this.#filePath = this.getLogFilePath();
        this.reIndex();
    }
    /**
     * @inheritdoc
     */
    write(entry) {
        const fd = fs.openSync(this.#filePath, "a");
        try {
            const msg = this.#options.format(entry);
            fs.writeSync(fd, msg + "\n");
            this.#size += msg.length;
        }
        finally {
            fs.closeSync(fd);
        }
        if (this.#size >= this.#options.maxSize) {
            this.reIndex();
            this.#size = 0;
        }
    }
    /**
     * Gets the file path to an indexed log file.
     * @param index Optional index of the log file to be included as part of the file name.
     * @returns File path that represents the indexed log file.
     */
    getLogFilePath(index = 0) {
        return path.join(this.#options.dest, `${this.#options.fileName}.${index}.log`);
    }
    /**
     * Gets the log files associated with this file target, including past and present.
     * @returns Log file entries.
     */
    getLogFiles() {
        const regex = /^\.(\d+)\.log$/;
        return fs
            .readdirSync(this.#options.dest, { withFileTypes: true })
            .reduce((prev, entry) => {
            if (entry.isDirectory() || entry.name.indexOf(this.#options.fileName) < 0) {
                return prev;
            }
            const match = entry.name.substring(this.#options.fileName.length).match(regex);
            if (match?.length !== 2) {
                return prev;
            }
            prev.push({
                path: path.join(this.#options.dest, entry.name),
                index: parseInt(match[1]),
            });
            return prev;
        }, [])
            .sort(({ index: a }, { index: b }) => {
            return a < b ? -1 : a > b ? 1 : 0;
        });
    }
    /**
     * Re-indexes the existing log files associated with this file target, removing old log files whose index exceeds the {@link FileTargetOptions.maxFileCount}, and renaming the
     * remaining log files, leaving index "0" free for a new log file.
     */
    reIndex() {
        // When the destination directory is new, create it, and return.
        if (!fs.existsSync(this.#options.dest)) {
            fs.mkdirSync(this.#options.dest);
            return;
        }
        const logFiles = this.getLogFiles();
        for (let i = logFiles.length - 1; i >= 0; i--) {
            const log = logFiles[i];
            if (i >= this.#options.maxFileCount - 1) {
                fs.rmSync(log.path);
            }
            else {
                fs.renameSync(log.path, this.getLogFilePath(i + 1));
            }
        }
    }
}

let __isDebugMode = undefined;
/**
 * Determines whether the current plugin is running in a debug environment; this is determined by the command-line arguments supplied to the plugin by Stream. Specifically, the result
 * is `true` when  either `--inspect`, `--inspect-brk` or `--inspect-port` are present as part of the processes' arguments.
 * @returns `true` when the plugin is running in debug mode; otherwise `false`.
 */
function isDebugMode() {
    if (__isDebugMode === undefined) {
        __isDebugMode = process.execArgv.some((arg) => {
            const name = arg.split("=")[0];
            return name === "--inspect" || name === "--inspect-brk" || name === "--inspect-port";
        });
    }
    return __isDebugMode;
}
/**
 * Gets the plugin's unique-identifier from the current working directory.
 * @returns The plugin's unique-identifier.
 */
function getPluginUUID() {
    const name = path.basename(process.cwd());
    const suffixIndex = name.lastIndexOf(".sdPlugin");
    return suffixIndex < 0 ? name : name.substring(0, suffixIndex);
}

// Log all entires to a log file.
const fileTarget = new FileTarget({
    dest: path.join(cwd(), "logs"),
    fileName: getPluginUUID(),
    format: stringFormatter(),
    maxFileCount: 10,
    maxSize: 50 * 1024 * 1024,
});
// Construct the log targets.
const targets = [fileTarget];
if (isDebugMode()) {
    targets.splice(0, 0, new ConsoleTarget());
}
/**
 * Logger responsible for capturing log messages.
 */
const logger$1 = new Logger({
    level: isDebugMode() ? "debug" : "info",
    minimumLevel: isDebugMode() ? "trace" : "debug",
    targets,
});
process.once("uncaughtException", (err) => logger$1.error("Process encountered uncaught exception", err));

/**
 * Provides a connection between the plugin and the Stream Deck allowing for messages to be sent and received.
 */
class Connection extends EventEmitter {
    /**
     * Private backing field for {@link Connection.registrationParameters}.
     */
    _registrationParameters;
    /**
     * Private backing field for {@link Connection.version}.
     */
    _version;
    /**
     * Used to ensure {@link Connection.connect} is invoked as a singleton; `false` when a connection is occurring or established.
     */
    canConnect = true;
    /**
     * Underlying web socket connection.
     */
    connection = withResolvers();
    /**
     * Logger scoped to the connection.
     */
    logger = logger$1.createScope("Connection");
    /**
     * Underlying connection information provided to the plugin to establish a connection with Stream Deck.
     * @returns The registration parameters.
     */
    get registrationParameters() {
        return (this._registrationParameters ??= this.getRegistrationParameters());
    }
    /**
     * Version of Stream Deck this instance is connected to.
     * @returns The version.
     */
    get version() {
        return (this._version ??= new Version(this.registrationParameters.info.application.version));
    }
    /**
     * Establishes a connection with the Stream Deck, allowing for the plugin to send and receive messages.
     * @returns A promise that is resolved when a connection has been established.
     */
    async connect() {
        // Ensure we only establish a single connection.
        if (this.canConnect) {
            this.canConnect = false;
            const webSocket = new WebSocket(`ws://127.0.0.1:${this.registrationParameters.port}`);
            webSocket.onmessage = (ev) => this.tryEmit(ev);
            webSocket.onopen = () => {
                webSocket.send(JSON.stringify({
                    event: this.registrationParameters.registerEvent,
                    uuid: this.registrationParameters.pluginUUID,
                }));
                // Web socket established a connection with the Stream Deck and the plugin was registered.
                this.connection.resolve(webSocket);
                this.emit("connected", this.registrationParameters.info);
            };
        }
        await this.connection.promise;
    }
    /**
     * Sends the commands to the Stream Deck, once the connection has been established and registered.
     * @param command Command being sent.
     * @returns `Promise` resolved when the command is sent to Stream Deck.
     */
    async send(command) {
        const connection = await this.connection.promise;
        const message = JSON.stringify(command);
        this.logger.trace(message);
        connection.send(message);
    }
    /**
     * Gets the registration parameters, provided by Stream Deck, that provide information to the plugin, including how to establish a connection.
     * @returns Parsed registration parameters.
     */
    getRegistrationParameters() {
        const params = {
            port: undefined,
            info: undefined,
            pluginUUID: undefined,
            registerEvent: undefined,
        };
        const scopedLogger = logger$1.createScope("RegistrationParameters");
        for (let i = 0; i < process.argv.length - 1; i++) {
            const param = process.argv[i];
            const value = process.argv[++i];
            switch (param) {
                case RegistrationParameter.Port:
                    scopedLogger.debug(`port=${value}`);
                    params.port = value;
                    break;
                case RegistrationParameter.PluginUUID:
                    scopedLogger.debug(`pluginUUID=${value}`);
                    params.pluginUUID = value;
                    break;
                case RegistrationParameter.RegisterEvent:
                    scopedLogger.debug(`registerEvent=${value}`);
                    params.registerEvent = value;
                    break;
                case RegistrationParameter.Info:
                    scopedLogger.debug(`info=${value}`);
                    params.info = JSON.parse(value);
                    break;
                default:
                    i--;
                    break;
            }
        }
        const invalidArgs = [];
        const validate = (name, value) => {
            if (value === undefined) {
                invalidArgs.push(name);
            }
        };
        validate(RegistrationParameter.Port, params.port);
        validate(RegistrationParameter.PluginUUID, params.pluginUUID);
        validate(RegistrationParameter.RegisterEvent, params.registerEvent);
        validate(RegistrationParameter.Info, params.info);
        if (invalidArgs.length > 0) {
            throw new Error(`Unable to establish a connection with Stream Deck, missing command line arguments: ${invalidArgs.join(", ")}`);
        }
        return params;
    }
    /**
     * Attempts to emit the {@link ev} that was received from the {@link Connection.connection}.
     * @param ev Event message data received from Stream Deck.
     */
    tryEmit(ev) {
        try {
            const message = JSON.parse(ev.data.toString());
            if (message.event) {
                this.logger.trace(ev.data.toString());
                this.emit(message.event, message);
            }
            else {
                this.logger.warn(`Received unknown message: ${ev.data}`);
            }
        }
        catch (err) {
            this.logger.error(`Failed to parse message: ${ev.data}`, err);
        }
    }
}
const connection = new Connection();

/**
 * Provides information for events received from Stream Deck.
 */
class Event {
    /**
     * Event that occurred.
     */
    type;
    /**
     * Initializes a new instance of the {@link Event} class.
     * @param source Source of the event, i.e. the original message from Stream Deck.
     */
    constructor(source) {
        this.type = source.event;
    }
}

/**
 * Provides information for an event relating to an action.
 */
class ActionWithoutPayloadEvent extends Event {
    action;
    /**
     * Initializes a new instance of the {@link ActionWithoutPayloadEvent} class.
     * @param action Action that raised the event.
     * @param source Source of the event, i.e. the original message from Stream Deck.
     */
    constructor(action, source) {
        super(source);
        this.action = action;
    }
}
/**
 * Provides information for an event relating to an action.
 */
class ActionEvent extends ActionWithoutPayloadEvent {
    /**
     * Provides additional information about the event that occurred, e.g. how many `ticks` the dial was rotated, the current `state` of the action, etc.
     */
    payload;
    /**
     * Initializes a new instance of the {@link ActionEvent} class.
     * @param action Action that raised the event.
     * @param source Source of the event, i.e. the original message from Stream Deck.
     */
    constructor(action, source) {
        super(action, source);
        this.payload = source.payload;
    }
}

const manifest$1 = new Lazy(() => {
    const path = join(process.cwd(), "manifest.json");
    if (!existsSync(path)) {
        throw new Error("Failed to read manifest.json as the file does not exist.");
    }
    try {
        return JSON.parse(readFileSync(path, {
            encoding: "utf-8",
            flag: "r",
        }).toString());
    }
    catch (e) {
        if (e instanceof SyntaxError) {
            return null;
        }
        else {
            throw e;
        }
    }
});
const softwareMinimumVersion = new Lazy(() => {
    if (manifest$1.value === null) {
        return null;
    }
    return new Version(manifest$1.value.Software.MinimumVersion);
});
/**
 * Gets the SDK version that the plugin requires.
 * @returns SDK version; otherwise `null` when the plugin is DRM protected.
 */
function getSDKVersion() {
    return manifest$1.value?.SDKVersion ?? null;
}
/**
 * Gets the minimum version that the plugin requires.
 * @returns Minimum required version; otherwise `null` when the plugin is DRM protected.
 */
function getSoftwareMinimumVersion() {
    return softwareMinimumVersion.value;
}
/**
 * Gets the manifest associated with the plugin.
 * @returns The manifest; otherwise `null` when the plugin is DRM protected.
 */
function getManifest() {
    return manifest$1.value;
}

const __items$1 = new Map();
/**
 * Provides a read-only store of Stream Deck devices.
 */
class ReadOnlyActionStore extends Enumerable {
    /**
     * Initializes a new instance of the {@link ReadOnlyActionStore}.
     */
    constructor() {
        super(__items$1);
    }
    /**
     * Gets the action with the specified identifier.
     * @param id Identifier of action to search for.
     * @returns The action, when present; otherwise `undefined`.
     */
    getActionById(id) {
        return __items$1.get(id);
    }
}
/**
 * Provides a store of Stream Deck actions.
 */
class ActionStore extends ReadOnlyActionStore {
    /**
     * Deletes the action from the store.
     * @param id The action's identifier.
     */
    delete(id) {
        __items$1.delete(id);
    }
    /**
     * Adds the action to the store.
     * @param action The action.
     */
    set(action) {
        __items$1.set(action.id, action);
    }
}
/**
 * Singleton instance of the action store.
 */
const actionStore = new ActionStore();

/**
 * Provides information for events relating to an application.
 */
class ApplicationEvent extends Event {
    /**
     * Monitored application that was launched/terminated.
     */
    application;
    /**
     * Initializes a new instance of the {@link ApplicationEvent} class.
     * @param source Source of the event, i.e. the original message from Stream Deck.
     */
    constructor(source) {
        super(source);
        this.application = source.payload.application;
    }
}

/**
 * Provides information for events relating to a device.
 */
class DeviceEvent extends Event {
    device;
    /**
     * Initializes a new instance of the {@link DeviceEvent} class.
     * @param source Source of the event, i.e. the original message from Stream Deck.
     * @param device Device that event is associated with.
     */
    constructor(source, device) {
        super(source);
        this.device = device;
    }
}

/**
 * Event information received from Stream Deck as part of a deep-link message being routed to the plugin.
 */
class DidReceiveDeepLinkEvent extends Event {
    /**
     * Deep-link URL routed from Stream Deck.
     */
    url;
    /**
     * Initializes a new instance of the {@link DidReceiveDeepLinkEvent} class.
     * @param source Source of the event, i.e. the original message from Stream Deck.
     */
    constructor(source) {
        super(source);
        this.url = new DeepLinkURL(source.payload.url);
    }
}
const PREFIX = "streamdeck://";
/**
 * Provides information associated with a URL received as part of a deep-link message, conforming to the URI syntax defined within RFC-3986 (https://datatracker.ietf.org/doc/html/rfc3986#section-3).
 */
class DeepLinkURL {
    /**
     * Fragment of the URL, with the number sign (#) omitted. For example, a URL of "/test#heading" would result in a {@link DeepLinkURL.fragment} of "heading".
     */
    fragment;
    /**
     * Original URL. For example, a URL of "/test?one=two#heading" would result in a {@link DeepLinkURL.href} of "/test?one=two#heading".
     */
    href;
    /**
     * Path of the URL; the full URL with the query and fragment omitted. For example, a URL of "/test?one=two#heading" would result in a {@link DeepLinkURL.path} of "/test".
     */
    path;
    /**
     * Query of the URL, with the question mark (?) omitted. For example, a URL of "/test?name=elgato&key=123" would result in a {@link DeepLinkURL.query} of "name=elgato&key=123".
     * See also {@link DeepLinkURL.queryParameters}.
     */
    query;
    /**
     * Query string parameters parsed from the URL. See also {@link DeepLinkURL.query}.
     */
    queryParameters;
    /**
     * Initializes a new instance of the {@link DeepLinkURL} class.
     * @param url URL of the deep-link, with the schema and authority omitted.
     */
    constructor(url) {
        const refUrl = new URL(`${PREFIX}${url}`);
        this.fragment = refUrl.hash.substring(1);
        this.href = refUrl.href.substring(PREFIX.length);
        this.path = DeepLinkURL.parsePath(this.href);
        this.query = refUrl.search.substring(1);
        this.queryParameters = refUrl.searchParams;
    }
    /**
     * Parses the {@link DeepLinkURL.path} from the specified {@link href}.
     * @param href Partial URL that contains the path to parse.
     * @returns The path of the URL.
     */
    static parsePath(href) {
        const indexOf = (char) => {
            const index = href.indexOf(char);
            return index >= 0 ? index : href.length;
        };
        return href.substring(0, Math.min(indexOf("?"), indexOf("#")));
    }
}

/**
 * Provides event information for when the plugin received the global settings.
 */
class DidReceiveGlobalSettingsEvent extends Event {
    /**
     * Settings associated with the event.
     */
    settings;
    /**
     * Initializes a new instance of the {@link DidReceiveGlobalSettingsEvent} class.
     * @param source Source of the event, i.e. the original message from Stream Deck.
     */
    constructor(source) {
        super(source);
        this.settings = source.payload.settings;
    }
}

/**
 * Provides information for an event triggered by a message being sent to the plugin, from the property inspector.
 */
class SendToPluginEvent extends Event {
    action;
    /**
     * Payload sent from the property inspector.
     */
    payload;
    /**
     * Initializes a new instance of the {@link SendToPluginEvent} class.
     * @param action Action that raised the event.
     * @param source Source of the event, i.e. the original message from Stream Deck.
     */
    constructor(action, source) {
        super(source);
        this.action = action;
        this.payload = source.payload;
    }
}

/**
 * Validates the `SDKVersion` within the manifest fulfils the minimum required version for the specified
 * feature; when the version is not fulfilled, an error is thrown with the feature formatted into the message.
 * @param minimumVersion Minimum required SDKVersion.
 * @param feature Feature that requires the version.
 */
function requiresSDKVersion(minimumVersion, feature) {
    const sdkVersion = getSDKVersion();
    if (sdkVersion !== null && minimumVersion > sdkVersion) {
        throw new Error(`[ERR_NOT_SUPPORTED]: ${feature} requires manifest SDK version ${minimumVersion} or higher, but found version ${sdkVersion}; please update the "SDKVersion" in the plugin's manifest to ${minimumVersion} or higher.`);
    }
}
/**
 * Validates the {@link streamDeckVersion} and manifest's `Software.MinimumVersion` are at least the {@link minimumVersion};
 * when the version is not fulfilled, an error is thrown with the {@link feature} formatted into the message.
 * @param minimumVersion Minimum required version.
 * @param streamDeckVersion Actual application version.
 * @param feature Feature that requires the version.
 */
function requiresVersion(minimumVersion, streamDeckVersion, feature) {
    const required = {
        major: Math.floor(minimumVersion),
        minor: Number(minimumVersion.toString().split(".").at(1) ?? 0), // Account for JavaScript's floating point precision.
        patch: 0,
        build: 0,
    };
    if (streamDeckVersion.compareTo(required) === -1) {
        throw new Error(`[ERR_NOT_SUPPORTED]: ${feature} requires Stream Deck version ${required.major}.${required.minor} or higher, but current version is ${streamDeckVersion.major}.${streamDeckVersion.minor}; please update Stream Deck and the "Software.MinimumVersion" in the plugin's manifest to "${required.major}.${required.minor}" or higher.`);
    }
    const softwareMinimumVersion = getSoftwareMinimumVersion();
    if (softwareMinimumVersion !== null && softwareMinimumVersion.compareTo(required) === -1) {
        throw new Error(`[ERR_NOT_SUPPORTED]: ${feature} requires Stream Deck version ${required.major}.${required.minor} or higher; please update the "Software.MinimumVersion" in the plugin's manifest to "${required.major}.${required.minor}" or higher.`);
    }
}

let __useExperimentalMessageIdentifiers = false;
const settings = {
    /**
     * Available from Stream Deck 7.1; determines whether message identifiers should be sent when getting
     * action-instance or global settings.
     *
     * When `true`, the did-receive events associated with settings are only emitted when the action-instance
     * or global settings are changed in the property inspector.
     * @returns The value.
     */
    get useExperimentalMessageIdentifiers() {
        return __useExperimentalMessageIdentifiers;
    },
    /**
     * Available from Stream Deck 7.1; determines whether message identifiers should be sent when getting
     * action-instance or global settings.
     *
     * When `true`, the did-receive events associated with settings are only emitted when the action-instance
     * or global settings are changed in the property inspector.
     */
    set useExperimentalMessageIdentifiers(value) {
        requiresVersion(7.1, connection.version, "Message identifiers");
        __useExperimentalMessageIdentifiers = value;
    },
    /**
     * Gets the global settings associated with the plugin.
     * @template T The type of global settings associated with the plugin.
     * @returns Promise containing the plugin's global settings.
     */
    getGlobalSettings: () => {
        return new Promise((resolve) => {
            connection.once("didReceiveGlobalSettings", (ev) => resolve(ev.payload.settings));
            connection.send({
                event: "getGlobalSettings",
                context: connection.registrationParameters.pluginUUID,
                id: randomUUID(),
            });
        });
    },
    /**
     * Occurs when the global settings are requested, or when the the global settings were updated in
     * the property inspector.
     * @template T The type of settings associated with the action.
     * @param listener Function to be invoked when the event occurs.
     * @returns A disposable that removes the listener.
     */
    onDidReceiveGlobalSettings: (listener) => {
        return connection.disposableOn("didReceiveGlobalSettings", (ev) => {
            // Do nothing when the global settings were requested.
            if (settings.useExperimentalMessageIdentifiers && ev.id) {
                return;
            }
            listener(new DidReceiveGlobalSettingsEvent(ev));
        });
    },
    /**
     * Occurs when the settings associated with an action instance are requested, or when the the settings
     * were updated in the property inspector.
     * @template T The type of settings associated with the action.
     * @param listener Function to be invoked when the event occurs.
     * @returns A disposable that removes the listener.
     */
    onDidReceiveSettings: (listener) => {
        return connection.disposableOn("didReceiveSettings", (ev) => {
            // Do nothing when the action's settings were requested.
            if (settings.useExperimentalMessageIdentifiers && ev.id) {
                return;
            }
            const action = actionStore.getActionById(ev.context);
            if (action) {
                listener(new ActionEvent(action, ev));
            }
        });
    },
    /**
     * Sets the global settings associated the plugin; these settings are only available to this plugin,
     * and should be used to persist information securely.
     * @param settings Settings to save.
     * @example
     * streamDeck.settings.setGlobalSettings({
     *   apiKey,
     *   connectedDate: new Date()
     * })
     */
    setGlobalSettings: async (settings) => {
        await connection.send({
            event: "setGlobalSettings",
            context: connection.registrationParameters.pluginUUID,
            payload: settings,
        });
    },
};

/**
 * Controller capable of sending/receiving payloads with the property inspector, and listening for events.
 */
class UIController {
    /**
     * Action associated with the current property inspector.
     */
    #action;
    /**
     * To overcome event races, the debounce counter keeps track of appear vs disappear events, ensuring
     * we only clear the current ui when an equal number of matching disappear events occur.
     */
    #appearanceStackCount = 0;
    /**
     * Initializes a new instance of the {@link UIController} class.
     */
    constructor() {
        // Track the action for the current property inspector.
        this.onDidAppear((ev) => {
            if (this.#isCurrent(ev.action)) {
                this.#appearanceStackCount++;
            }
            else {
                this.#appearanceStackCount = 1;
                this.#action = ev.action;
            }
        });
        this.onDidDisappear((ev) => {
            if (this.#isCurrent(ev.action)) {
                this.#appearanceStackCount--;
                if (this.#appearanceStackCount <= 0) {
                    this.#action = undefined;
                }
            }
        });
    }
    /**
     * Gets the action associated with the current property.
     * @returns The action; otherwise `undefined` when a property inspector is not visible.
     */
    get action() {
        return this.#action;
    }
    /**
     * Occurs when the property inspector associated with the action becomes visible, i.e. the user
     * selected an action in the Stream Deck application..
     * @template T The type of settings associated with the action.
     * @param listener Function to be invoked when the event occurs.
     * @returns A disposable that, when disposed, removes the listener.
     */
    onDidAppear(listener) {
        return connection.disposableOn("propertyInspectorDidAppear", (ev) => {
            const action = actionStore.getActionById(ev.context);
            if (action) {
                listener(new ActionWithoutPayloadEvent(action, ev));
            }
        });
    }
    /**
     * Occurs when the property inspector associated with the action disappears, i.e. the user unselected
     * the action in the Stream Deck application.
     * @template T The type of settings associated with the action.
     * @param listener Function to be invoked when the event occurs.
     * @returns A disposable that, when disposed, removes the listener.
     */
    onDidDisappear(listener) {
        return connection.disposableOn("propertyInspectorDidDisappear", (ev) => {
            const action = actionStore.getActionById(ev.context);
            if (action) {
                listener(new ActionWithoutPayloadEvent(action, ev));
            }
        });
    }
    /**
     * Occurs when a message was sent to the plugin _from_ the property inspector.
     * @template TPayload The type of the payload received from the property inspector.
     * @template TSettings The type of settings associated with the action.
     * @param listener Function to be invoked when the event occurs.
     * @returns A disposable that, when disposed, removes the listener.
     */
    onSendToPlugin(listener) {
        return connection.disposableOn("sendToPlugin", (ev) => {
            const action = actionStore.getActionById(ev.context);
            if (action) {
                listener(new SendToPluginEvent(action, ev));
            }
        });
    }
    /**
     * Sends the payload to the property inspector; the payload is only sent when the property inspector
     * is visible for an action provided by this plugin.
     * @param payload Payload to send.
     */
    async sendToPropertyInspector(payload) {
        if (this.#action) {
            await connection.send({
                event: "sendToPropertyInspector",
                context: this.#action.id,
                payload,
            });
        }
    }
    /**
     * Determines whether the specified action is the action for the current property inspector.
     * @param action Action to check against.
     * @returns `true` when the actions are the same.
     */
    #isCurrent(action) {
        return (this.#action?.id === action.id &&
            this.#action?.manifestId === action.manifestId &&
            this.#action?.device?.id === action.device.id);
    }
}
const ui = new UIController();

const __items = new Map();
/**
 * Provides a read-only store of Stream Deck devices.
 */
class ReadOnlyDeviceStore extends Enumerable {
    /**
     * Initializes a new instance of the {@link ReadOnlyDeviceStore}.
     */
    constructor() {
        super(__items);
    }
    /**
     * Gets the Stream Deck {@link Device} associated with the specified {@link deviceId}.
     * @param deviceId Identifier of the Stream Deck device.
     * @returns The Stream Deck device information; otherwise `undefined` if a device with the {@link deviceId} does not exist.
     */
    getDeviceById(deviceId) {
        return __items.get(deviceId);
    }
}
/**
 * Provides a store of Stream Deck devices.
 */
class DeviceStore extends ReadOnlyDeviceStore {
    /**
     * Adds the device to the store.
     * @param device The device.
     */
    set(device) {
        __items.set(device.id, device);
    }
}
/**
 * Singleton instance of the device store.
 */
const deviceStore = new DeviceStore();

/**
 * Provides information about an instance of a Stream Deck action.
 */
class ActionContext {
    /**
     * Device the action is associated with.
     */
    #device;
    /**
     * Source of the action.
     */
    #source;
    /**
     * Initializes a new instance of the {@link ActionContext} class.
     * @param source Source of the action.
     */
    constructor(source) {
        this.#source = source;
        const device = deviceStore.getDeviceById(source.device);
        if (!device) {
            throw new Error(`Failed to initialize action; device ${source.device} not found`);
        }
        this.#device = device;
    }
    /**
     * Type of the action.
     * - `Keypad` is a key.
     * - `Encoder` is a dial and portion of the touch strip.
     * @returns Controller type.
     */
    get controllerType() {
        return this.#source.payload.controller;
    }
    /**
     * Stream Deck device the action is positioned on.
     * @returns Stream Deck device.
     */
    get device() {
        return this.#device;
    }
    /**
     * Action instance identifier.
     * @returns Identifier.
     */
    get id() {
        return this.#source.context;
    }
    /**
     * Manifest identifier (UUID) for this action type.
     * @returns Manifest identifier.
     */
    get manifestId() {
        return this.#source.action;
    }
    /**
     * Converts this instance to a serializable object.
     * @returns The serializable object.
     */
    toJSON() {
        return {
            controllerType: this.controllerType,
            device: this.device,
            id: this.id,
            manifestId: this.manifestId,
        };
    }
}

const REQUEST_TIMEOUT = 15 * 1000; // 15s
/**
 * Provides a contextualized instance of an {@link Action}, allowing for direct communication with the Stream Deck.
 * @template T The type of settings associated with the action.
 */
let Action$1 = class Action extends ActionContext {
    /**
     * Gets the resources (files) associated with this action; these resources are embedded into the
     * action when it is exported, either individually, or as part of a profile.
     *
     * Available from Stream Deck 7.1.
     * @returns The resources.
     */
    async getResources() {
        requiresVersion(7.1, connection.version, "getResources");
        const res = await this.#fetch("getResources", "didReceiveResources");
        return res.payload.resources;
    }
    /**
     * Gets the settings associated this action instance.
     * @template U The type of settings associated with the action.D
     * @returns Promise containing the action instance's settings.
     */
    async getSettings() {
        const res = await this.#fetch("getSettings", "didReceiveSettings");
        return res.payload.settings;
    }
    /**
     * Determines whether this instance is a dial.
     * @returns `true` when this instance is a dial; otherwise `false`.
     */
    isDial() {
        return this.controllerType === "Encoder";
    }
    /**
     * Determines whether this instance is a key.
     * @returns `true` when this instance is a key; otherwise `false`.
     */
    isKey() {
        return this.controllerType === "Keypad";
    }
    /**
     * Sets the resources (files) associated with this action; these resources are embedded into the
     * action when it is exported, either individually, or as part of a profile.
     *
     * Available from Stream Deck 7.1.
     * @example
     * action.setResources({
     *   fileOne: "c:\\hello-world.txt",
     *   anotherFile: "c:\\icon.png"
     * });
     * @param resources The resources as a map of file paths.
     * @returns `Promise` resolved when the resources are saved to Stream Deck.
     */
    setResources(resources) {
        requiresVersion(7.1, connection.version, "setResources");
        return connection.send({
            event: "setResources",
            context: this.id,
            payload: resources,
        });
    }
    /**
     * Sets the {@link settings} associated with this action instance. Use in conjunction with {@link Action.getSettings}.
     * @param settings Settings to persist.
     * @returns `Promise` resolved when the {@link settings} are sent to Stream Deck.
     */
    setSettings(settings) {
        return connection.send({
            event: "setSettings",
            context: this.id,
            payload: settings,
        });
    }
    /**
     * Temporarily shows an alert (i.e. warning), in the form of an exclamation mark in a yellow triangle, on this action instance. Used to provide visual feedback when an action failed.
     * @returns `Promise` resolved when the request to show an alert has been sent to Stream Deck.
     */
    showAlert() {
        return connection.send({
            event: "showAlert",
            context: this.id,
        });
    }
    /**
     * Fetches information from Stream Deck by sending the command, and awaiting the event.
     * @param command Name of the event (command) to send.
     * @param event Name of the event to await.
     * @returns The payload from the received event.
     */
    async #fetch(command, event) {
        const { resolve, reject, promise } = withResolvers();
        // Set a timeout to prevent endless awaiting.
        const timeoutId = setTimeout(() => {
            listener.dispose();
            reject("The request timed out");
        }, REQUEST_TIMEOUT);
        // Listen for an event that can resolve the request.
        const listener = connection.disposableOn(event, (ev) => {
            // Make sure the received event is for this action.
            if (ev.context == this.id) {
                clearTimeout(timeoutId);
                listener.dispose();
                resolve(ev);
            }
        });
        // Send the request; specifying an id signifies its a request.
        await connection.send({
            event: command,
            context: this.id,
            id: randomUUID(),
        });
        return promise;
    }
};

/**
 * Provides a contextualized instance of a dial action.
 * @template T The type of settings associated with the action.
 */
class DialAction extends Action$1 {
    /**
     * Private backing field for {@link DialAction.coordinates}.
     */
    #coordinates;
    /**
     * Initializes a new instance of the {@see DialAction} class.
     * @param source Source of the action.
     */
    constructor(source) {
        super(source);
        if (source.payload.controller !== "Encoder") {
            throw new Error("Unable to create DialAction; source event is not a Encoder");
        }
        this.#coordinates = Object.freeze(source.payload.coordinates);
    }
    /**
     * Coordinates of the dial.
     * @returns The coordinates.
     */
    get coordinates() {
        return this.#coordinates;
    }
    /**
     * Sets the feedback for the current layout associated with this action instance, allowing for the visual items to be updated. Layouts are a powerful way to provide dynamic information
     * to users, and can be assigned in the manifest, or dynamically via {@link Action.setFeedbackLayout}.
     *
     * The {@link feedback} payload defines which items within the layout will be updated, and are identified by their property name (defined as the `key` in the layout's definition).
     * The values can either by a complete new definition, a `string` for layout item types of `text` and `pixmap`, or a `number` for layout item types of `bar` and `gbar`.
     * @param feedback Object containing information about the layout items to be updated.
     * @returns `Promise` resolved when the request to set the {@link feedback} has been sent to Stream Deck.
     */
    setFeedback(feedback) {
        return connection.send({
            event: "setFeedback",
            context: this.id,
            payload: feedback,
        });
    }
    /**
     * Sets the layout associated with this action instance. The layout must be either a built-in layout identifier, or path to a local layout JSON file within the plugin's folder.
     * Use in conjunction with {@link Action.setFeedback} to update the layout's current items' settings.
     * @param layout Name of a pre-defined layout, or relative path to a custom one.
     * @returns `Promise` resolved when the new layout has been sent to Stream Deck.
     */
    setFeedbackLayout(layout) {
        return connection.send({
            event: "setFeedbackLayout",
            context: this.id,
            payload: {
                layout,
            },
        });
    }
    /**
     * Sets the {@link image} to be display for this action instance within Stream Deck app.
     *
     * NB: The image can only be set by the plugin when the the user has not specified a custom image.
     * @param image Image to display; this can be either a path to a local file within the plugin's folder, a base64 encoded `string` with the mime type declared (e.g. PNG, JPEG, etc.),
     * or an SVG `string`. When `undefined`, the image from the manifest will be used.
     * @returns `Promise` resolved when the request to set the {@link image} has been sent to Stream Deck.
     */
    setImage(image) {
        return connection.send({
            event: "setImage",
            context: this.id,
            payload: {
                image,
            },
        });
    }
    /**
     * Sets the {@link title} displayed for this action instance.
     *
     * NB: The title can only be set by the plugin when the the user has not specified a custom title.
     * @param title Title to display.
     * @returns `Promise` resolved when the request to set the {@link title} has been sent to Stream Deck.
     */
    setTitle(title) {
        return this.setFeedback({ title });
    }
    /**
     * Sets the trigger (interaction) {@link descriptions} associated with this action instance. Descriptions are shown within the Stream Deck application, and informs the user what
     * will happen when they interact with the action, e.g. rotate, touch, etc. When {@link descriptions} is `undefined`, the descriptions will be reset to the values provided as part
     * of the manifest.
     *
     * NB: Applies to encoders (dials / touchscreens) found on Stream Deck + devices.
     * @param descriptions Descriptions that detail the action's interaction.
     * @returns `Promise` resolved when the request to set the {@link descriptions} has been sent to Stream Deck.
     */
    setTriggerDescription(descriptions) {
        return connection.send({
            event: "setTriggerDescription",
            context: this.id,
            payload: descriptions || {},
        });
    }
    /**
     * @inheritdoc
     */
    toJSON() {
        return {
            ...super.toJSON(),
            coordinates: this.coordinates,
        };
    }
}

/**
 * Provides a contextualized instance of a key action.
 * @template T The type of settings associated with the action.
 */
class KeyAction extends Action$1 {
    /**
     * Private backing field for {@link KeyAction.coordinates}.
     */
    #coordinates;
    /**
     * Source of the action.
     */
    #source;
    /**
     * Initializes a new instance of the {@see KeyAction} class.
     * @param source Source of the action.
     */
    constructor(source) {
        super(source);
        if (source.payload.controller !== "Keypad") {
            throw new Error("Unable to create KeyAction; source event is not a Keypad");
        }
        this.#coordinates = !source.payload.isInMultiAction ? Object.freeze(source.payload.coordinates) : undefined;
        this.#source = source;
    }
    /**
     * Coordinates of the key; otherwise `undefined` when the action is part of a multi-action.
     * @returns The coordinates.
     */
    get coordinates() {
        return this.#coordinates;
    }
    /**
     * Determines whether the key is part of a multi-action.
     * @returns `true` when in a multi-action; otherwise `false`.
     */
    isInMultiAction() {
        return this.#source.payload.isInMultiAction;
    }
    /**
     * Sets the {@link image} to be display for this action instance.
     *
     * NB: The image can only be set by the plugin when the the user has not specified a custom image.
     * @param image Image to display; this can be either a path to a local file within the plugin's folder, a base64 encoded `string` with the mime type declared (e.g. PNG, JPEG, etc.),
     * or an SVG `string`. When `undefined`, the image from the manifest will be used.
     * @param options Additional options that define where and how the image should be rendered.
     * @returns `Promise` resolved when the request to set the {@link image} has been sent to Stream Deck.
     */
    setImage(image, options) {
        return connection.send({
            event: "setImage",
            context: this.id,
            payload: {
                image,
                ...options,
            },
        });
    }
    /**
     * Sets the current {@link state} of this action instance; only applies to actions that have multiple states defined within the manifest.
     * @param state State to set; this be either 0, or 1.
     * @returns `Promise` resolved when the request to set the state of an action instance has been sent to Stream Deck.
     */
    setState(state) {
        return connection.send({
            event: "setState",
            context: this.id,
            payload: {
                state,
            },
        });
    }
    /**
     * Sets the {@link title} displayed for this action instance.
     *
     * NB: The title can only be set by the plugin when the the user has not specified a custom title.
     * @param title Title to display; when `undefined` the title within the manifest will be used.
     * @param options Additional options that define where and how the title should be rendered.
     * @returns `Promise` resolved when the request to set the {@link title} has been sent to Stream Deck.
     */
    setTitle(title, options) {
        return connection.send({
            event: "setTitle",
            context: this.id,
            payload: {
                title,
                ...options,
            },
        });
    }
    /**
     * Temporarily shows an "OK" (i.e. success), in the form of a check-mark in a green circle, on this action instance. Used to provide visual feedback when an action successfully
     * executed.
     * @returns `Promise` resolved when the request to show an "OK" has been sent to Stream Deck.
     */
    showOk() {
        return connection.send({
            event: "showOk",
            context: this.id,
        });
    }
    /**
     * @inheritdoc
     */
    toJSON() {
        return {
            ...super.toJSON(),
            coordinates: this.coordinates,
            isInMultiAction: this.isInMultiAction(),
        };
    }
}

const manifest = new Lazy(() => getManifest());
/**
 * Provides functions, and information, for interacting with Stream Deck actions.
 */
class ActionService extends ReadOnlyActionStore {
    /**
     * Initializes a new instance of the {@link ActionService} class.
     */
    constructor() {
        super();
        // Adds the action to the store.
        connection.prependListener("willAppear", (ev) => {
            const action = ev.payload.controller === "Encoder" ? new DialAction(ev) : new KeyAction(ev);
            actionStore.set(action);
        });
        // Remove the action from the store.
        connection.prependListener("willDisappear", (ev) => actionStore.delete(ev.context));
    }
    /**
     * Occurs when the user presses a dial (Stream Deck +).
     * @template T The type of settings associated with the action.
     * @param listener Function to be invoked when the event occurs.
     * @returns A disposable that, when disposed, removes the listener.
     */
    onDialDown(listener) {
        return connection.disposableOn("dialDown", (ev) => {
            const action = actionStore.getActionById(ev.context);
            if (action?.isDial()) {
                listener(new ActionEvent(action, ev));
            }
        });
    }
    /**
     * Occurs when the user rotates a dial (Stream Deck +).
     * @template T The type of settings associated with the action.
     * @param listener Function to be invoked when the event occurs.
     * @returns A disposable that, when disposed, removes the listener.
     */
    onDialRotate(listener) {
        return connection.disposableOn("dialRotate", (ev) => {
            const action = actionStore.getActionById(ev.context);
            if (action?.isDial()) {
                listener(new ActionEvent(action, ev));
            }
        });
    }
    /**
     * Occurs when the user releases a pressed dial (Stream Deck +).
     * @template T The type of settings associated with the action.
     * @param listener Function to be invoked when the event occurs.
     * @returns A disposable that, when disposed, removes the listener.
     */
    onDialUp(listener) {
        return connection.disposableOn("dialUp", (ev) => {
            const action = actionStore.getActionById(ev.context);
            if (action?.isDial()) {
                listener(new ActionEvent(action, ev));
            }
        });
    }
    /**
     * Occurs when the resources were updated within the property inspector.
     * @param listener Function to be invoked when the event occurs.
     * @returns A disposable that, when disposed, removes the listener.
     */
    onDidReceiveResources(listener) {
        return connection.disposableOn("didReceiveResources", (ev) => {
            // When the id is defined, the resources were requested, so we don't propagate the event.
            if (ev.id !== undefined) {
                return;
            }
            const action = actionStore.getActionById(ev.context);
            if (action) {
                listener(new ActionEvent(action, ev));
            }
        });
    }
    /**
     * Occurs when the user presses a action down.
     * @template T The type of settings associated with the action.
     * @param listener Function to be invoked when the event occurs.
     * @returns A disposable that, when disposed, removes the listener.
     */
    onKeyDown(listener) {
        return connection.disposableOn("keyDown", (ev) => {
            const action = actionStore.getActionById(ev.context);
            if (action?.isKey()) {
                listener(new ActionEvent(action, ev));
            }
        });
    }
    /**
     * Occurs when the user releases a pressed action.
     * @template T The type of settings associated with the action.
     * @param listener Function to be invoked when the event occurs.
     * @returns A disposable that, when disposed, removes the listener.
     */
    onKeyUp(listener) {
        return connection.disposableOn("keyUp", (ev) => {
            const action = actionStore.getActionById(ev.context);
            if (action?.isKey()) {
                listener(new ActionEvent(action, ev));
            }
        });
    }
    /**
     * Occurs when the user updates an action's title settings in the Stream Deck application. See also {@link Action.setTitle}.
     * @template T The type of settings associated with the action.
     * @param listener Function to be invoked when the event occurs.
     * @returns A disposable that, when disposed, removes the listener.
     */
    onTitleParametersDidChange(listener) {
        return connection.disposableOn("titleParametersDidChange", (ev) => {
            const action = actionStore.getActionById(ev.context);
            if (action) {
                listener(new ActionEvent(action, ev));
            }
        });
    }
    /**
     * Occurs when the user taps the touchscreen (Stream Deck +).
     * @template T The type of settings associated with the action.
     * @param listener Function to be invoked when the event occurs.
     * @returns A disposable that, when disposed, removes the listener.
     */
    onTouchTap(listener) {
        return connection.disposableOn("touchTap", (ev) => {
            const action = actionStore.getActionById(ev.context);
            if (action?.isDial()) {
                listener(new ActionEvent(action, ev));
            }
        });
    }
    /**
     * Occurs when an action appears on the Stream Deck due to the user navigating to another page, profile, folder, etc. This also occurs during startup if the action is on the "front
     * page". An action refers to _all_ types of actions, e.g. keys, dials,
     * @template T The type of settings associated with the action.
     * @param listener Function to be invoked when the event occurs.
     * @returns A disposable that, when disposed, removes the listener.
     */
    onWillAppear(listener) {
        return connection.disposableOn("willAppear", (ev) => {
            const action = actionStore.getActionById(ev.context);
            if (action) {
                listener(new ActionEvent(action, ev));
            }
        });
    }
    /**
     * Occurs when an action disappears from the Stream Deck due to the user navigating to another page, profile, folder, etc. An action refers to _all_ types of actions, e.g. keys,
     * dials, touchscreens, pedals, etc.
     * @template T The type of settings associated with the action.
     * @param listener Function to be invoked when the event occurs.
     * @returns A disposable that, when disposed, removes the listener.
     */
    onWillDisappear(listener) {
        return connection.disposableOn("willDisappear", (ev) => listener(new ActionEvent(new ActionContext(ev), ev)));
    }
    /**
     * Registers the action with the Stream Deck, routing all events associated with the {@link SingletonAction.manifestId} to the specified {@link action}.
     * @param action The action to register.
     * @example
     * ＠action({ UUID: "com.elgato.test.action" })
     * class MyCustomAction extends SingletonAction {
     *     export function onKeyDown(ev: KeyDownEvent) {
     *         // Do some awesome thing.
     *     }
     * }
     *
     * streamDeck.actions.registerAction(new MyCustomAction());
     */
    registerAction(action) {
        if (action.manifestId === undefined) {
            throw new Error("The action's manifestId cannot be undefined.");
        }
        if (manifest.value !== null && !manifest.value.Actions.some((a) => a.UUID === action.manifestId)) {
            throw new Error(`The action's manifestId was not found within the manifest: ${action.manifestId}`);
        }
        // Routes an event to the action, when the applicable listener is defined on the action.
        const { manifestId } = action;
        const route = (fn, listener) => {
            const boundedListener = listener?.bind(action);
            if (boundedListener === undefined) {
                return;
            }
            fn.bind(action)(async (ev) => {
                if (ev.action.manifestId == manifestId) {
                    await boundedListener(ev);
                }
            });
        };
        // Route each of the action events.
        route(this.onDialDown, action.onDialDown);
        route(this.onDialUp, action.onDialUp);
        route(this.onDialRotate, action.onDialRotate);
        route(ui.onSendToPlugin, action.onSendToPlugin);
        route(this.onDidReceiveResources, action.onDidReceiveResources);
        route(settings.onDidReceiveSettings, action.onDidReceiveSettings);
        route(this.onKeyDown, action.onKeyDown);
        route(this.onKeyUp, action.onKeyUp);
        route(ui.onDidAppear, action.onPropertyInspectorDidAppear);
        route(ui.onDidDisappear, action.onPropertyInspectorDidDisappear);
        route(this.onTitleParametersDidChange, action.onTitleParametersDidChange);
        route(this.onTouchTap, action.onTouchTap);
        route(this.onWillAppear, action.onWillAppear);
        route(this.onWillDisappear, action.onWillDisappear);
    }
}
/**
 * Service for interacting with Stream Deck actions.
 */
const actionService = new ActionService();

/**
 * Provides information about a device.
 */
class Device {
    /**
     * Private backing field for {@link Device.isConnected}.
     */
    #isConnected = false;
    /**
     * Private backing field for the device's information.
     */
    #info;
    /**
     * Unique identifier of the device.
     */
    id;
    /**
     * Initializes a new instance of the {@link Device} class.
     * @param id Device identifier.
     * @param info Information about the device.
     * @param isConnected Determines whether the device is connected.
     */
    constructor(id, info, isConnected) {
        this.id = id;
        this.#info = info;
        this.#isConnected = isConnected;
        // Set connected.
        connection.prependListener("deviceDidConnect", (ev) => {
            if (ev.device === this.id) {
                this.#info = ev.deviceInfo;
                this.#isConnected = true;
            }
        });
        // Track changes.
        connection.prependListener("deviceDidChange", (ev) => {
            if (ev.device === this.id) {
                this.#info = ev.deviceInfo;
            }
        });
        // Set disconnected.
        connection.prependListener("deviceDidDisconnect", (ev) => {
            if (ev.device === this.id) {
                this.#isConnected = false;
            }
        });
    }
    /**
     * Actions currently visible on the device.
     * @returns Collection of visible actions.
     */
    get actions() {
        return actionStore.filter((a) => a.device.id === this.id);
    }
    /**
     * Determines whether the device is currently connected.
     * @returns `true` when the device is connected; otherwise `false`.
     */
    get isConnected() {
        return this.#isConnected;
    }
    /**
     * Name of the device, as specified by the user in the Stream Deck application.
     * @returns Name of the device.
     */
    get name() {
        return this.#info.name;
    }
    /**
     * Number of action slots, excluding dials / touchscreens, available to the device.
     * @returns Size of the device.
     */
    get size() {
        return this.#info.size;
    }
    /**
     * Type of the device that was connected, e.g. Stream Deck +, Stream Deck Pedal, etc. See {@link DeviceType}.
     * @returns Type of the device.
     */
    get type() {
        return this.#info.type;
    }
}

/**
 * Provides functions, and information, for interacting with Stream Deck actions.
 */
class DeviceService extends ReadOnlyDeviceStore {
    /**
     * Initializes a new instance of the {@link DeviceService}.
     */
    constructor() {
        super();
        // Add the devices from registration parameters.
        connection.once("connected", (info) => {
            info.devices.forEach((dev) => deviceStore.set(new Device(dev.id, dev, false)));
        });
        // Add new devices that were connected.
        connection.on("deviceDidConnect", ({ device: id, deviceInfo }) => {
            if (!deviceStore.getDeviceById(id)) {
                deviceStore.set(new Device(id, deviceInfo, true));
            }
        });
        // Add new devices that were changed (Virtual Stream Deck event race).
        connection.on("deviceDidChange", ({ device: id, deviceInfo }) => {
            if (!deviceStore.getDeviceById(id)) {
                deviceStore.set(new Device(id, deviceInfo, false));
            }
        });
    }
    /**
     * Occurs when a Stream Deck device changed, for example its name or size.
     *
     * Available from Stream Deck 7.0.
     * @param listener Function to be invoked when the event occurs.
     * @returns A disposable that, when disposed, removes the listener.
     */
    onDeviceDidChange(listener) {
        requiresVersion(7.0, connection.version, "onDeviceDidChange");
        return connection.disposableOn("deviceDidChange", (ev) => listener(new DeviceEvent(ev, this.getDeviceById(ev.device))));
    }
    /**
     * Occurs when a Stream Deck device is connected. See also {@link DeviceService.onDeviceDidConnect}.
     * @param listener Function to be invoked when the event occurs.
     * @returns A disposable that, when disposed, removes the listener.
     */
    onDeviceDidConnect(listener) {
        return connection.disposableOn("deviceDidConnect", (ev) => listener(new DeviceEvent(ev, this.getDeviceById(ev.device))));
    }
    /**
     * Occurs when a Stream Deck device is disconnected. See also {@link DeviceService.onDeviceDidDisconnect}.
     * @param listener Function to be invoked when the event occurs.
     * @returns A disposable that, when disposed, removes the listener.
     */
    onDeviceDidDisconnect(listener) {
        return connection.disposableOn("deviceDidDisconnect", (ev) => listener(new DeviceEvent(ev, this.getDeviceById(ev.device))));
    }
}
/**
 * Provides functions, and information, for interacting with Stream Deck actions.
 */
const deviceService = new DeviceService();

/**
 * Loads a locale from the file system.
 * @param language Language to load.
 * @returns Contents of the locale.
 */
function fileSystemLocaleProvider(language) {
    const filePath = path.join(process.cwd(), `${language}.json`);
    if (!fs.existsSync(filePath)) {
        return null;
    }
    try {
        // Parse the translations from the file.
        const contents = fs.readFileSync(filePath, { flag: "r" })?.toString();
        return parseLocalizations(contents);
    }
    catch (err) {
        logger$1.error(`Failed to load translations from ${filePath}`, err);
        return null;
    }
}
/**
 * Parses the localizations from the specified contents, or throws a `TypeError` when unsuccessful.
 * @param contents Contents that represent the stringified JSON containing the localizations.
 * @returns The localizations; otherwise a `TypeError`.
 */
function parseLocalizations(contents) {
    const json = JSON.parse(contents);
    if (json !== undefined && json !== null && typeof json === "object" && "Localization" in json) {
        return json["Localization"];
    }
    throw new TypeError(`Translations must be a JSON object nested under a property named "Localization"`);
}

/**
 * Requests the Stream Deck switches the current profile of the specified {@link deviceId} to the {@link profile}; when no {@link profile} is provided the previously active profile
 * is activated.
 *
 * NB: Plugins may only switch to profiles distributed with the plugin, as defined within the manifest, and cannot access user-defined profiles.
 * @param deviceId Unique identifier of the device where the profile should be set.
 * @param profile Optional name of the profile to switch to; when `undefined` the previous profile will be activated. Name must be identical to the one provided in the manifest.
 * @param page Optional page to show when switching to the {@link profile}, indexed from 0. When `undefined`, the page that was previously visible (when switching away from the
 * profile) will be made visible.
 * @returns `Promise` resolved when the request to switch the `profile` has been sent to Stream Deck.
 */
function switchToProfile(deviceId, profile, page) {
    if (page !== undefined) {
        requiresVersion(6.5, connection.version, "Switching to a profile page");
    }
    return connection.send({
        event: "switchToProfile",
        context: connection.registrationParameters.pluginUUID,
        device: deviceId,
        payload: {
            page,
            profile,
        },
    });
}

var profiles = /*#__PURE__*/Object.freeze({
    __proto__: null,
    switchToProfile: switchToProfile
});

/**
 * Occurs when a monitored application is launched. Monitored applications can be defined in the manifest via the {@link Manifest.ApplicationsToMonitor} property.
 * See also {@link onApplicationDidTerminate}.
 * @param listener Function to be invoked when the event occurs.
 * @returns A disposable that, when disposed, removes the listener.
 */
function onApplicationDidLaunch(listener) {
    return connection.disposableOn("applicationDidLaunch", (ev) => listener(new ApplicationEvent(ev)));
}
/**
 * Occurs when a monitored application terminates. Monitored applications can be defined in the manifest via the {@link Manifest.ApplicationsToMonitor} property.
 * See also {@link onApplicationDidLaunch}.
 * @param listener Function to be invoked when the event occurs.
 * @returns A disposable that, when disposed, removes the listener.
 */
function onApplicationDidTerminate(listener) {
    return connection.disposableOn("applicationDidTerminate", (ev) => listener(new ApplicationEvent(ev)));
}
/**
 * Occurs when a deep-link message is routed to the plugin from Stream Deck. One-way deep-link messages can be sent to plugins from external applications using the URL format
 * `streamdeck://plugins/message/<PLUGIN_UUID>/{MESSAGE}`.
 * @param listener Function to be invoked when the event occurs.
 * @returns A disposable that, when disposed, removes the listener.
 */
function onDidReceiveDeepLink(listener) {
    requiresVersion(6.5, connection.version, "Receiving deep-link messages");
    return connection.disposableOn("didReceiveDeepLink", (ev) => listener(new DidReceiveDeepLinkEvent(ev)));
}
/**
 * Occurs when the computer wakes up.
 * @param listener Function to be invoked when the event occurs.
 * @returns A disposable that, when disposed, removes the listener.
 */
function onSystemDidWakeUp(listener) {
    return connection.disposableOn("systemDidWakeUp", (ev) => listener(new Event(ev)));
}
/**
 * Opens the specified `url` in the user's default browser.
 * @param url URL to open.
 * @returns `Promise` resolved when the request to open the `url` has been sent to Stream Deck.
 */
function openUrl(url) {
    return connection.send({
        event: "openUrl",
        payload: {
            url,
        },
    });
}
/**
 * Gets the secrets associated with the plugin.
 * @returns `Promise` resolved with the secrets associated with the plugin.
 */
function getSecrets() {
    requiresVersion(6.9, connection.version, "Secrets");
    requiresSDKVersion(3, "Secrets");
    return new Promise((resolve) => {
        connection.once("didReceiveSecrets", (ev) => resolve(ev.payload.secrets));
        connection.send({
            event: "getSecrets",
            context: connection.registrationParameters.pluginUUID,
        });
    });
}

var system = /*#__PURE__*/Object.freeze({
    __proto__: null,
    getSecrets: getSecrets,
    onApplicationDidLaunch: onApplicationDidLaunch,
    onApplicationDidTerminate: onApplicationDidTerminate,
    onDidReceiveDeepLink: onDidReceiveDeepLink,
    onSystemDidWakeUp: onSystemDidWakeUp,
    openUrl: openUrl
});

/**
 * Defines a Stream Deck action associated with the plugin.
 * @param definition The definition of the action, e.g. it's identifier, name, etc.
 * @returns The definition decorator.
 */
function action(definition) {
    const manifestId = definition.UUID;
    // eslint-disable-next-line @typescript-eslint/explicit-function-return-type, @typescript-eslint/no-unused-vars
    return function (target, context) {
        return class extends target {
            /**
             * The universally-unique value that identifies the action within the manifest.
             */
            manifestId = manifestId;
        };
    };
}

/**
 * Provides the main bridge between the plugin and the Stream Deck allowing the plugin to send requests and receive events, e.g. when the user presses an action.
 * @template T The type of settings associated with the action.
 */
class SingletonAction {
    /**
     * The universally-unique value that identifies the action within the manifest.
     */
    manifestId;
    /**
     * Gets the visible actions with the `manifestId` that match this instance's.
     * @returns The visible actions.
     */
    get actions() {
        return actionStore.filter((a) => a.manifestId === this.manifestId);
    }
}

let i18n;
const streamDeck = {
    /**
     * Namespace for event listeners and functionality relating to Stream Deck actions.
     * @returns Actions namespace.
     */
    get actions() {
        return actionService;
    },
    /**
     * Namespace for interacting with Stream Deck devices.
     * @returns Devices namespace.
     */
    get devices() {
        return deviceService;
    },
    /**
     * Internalization provider, responsible for managing localizations and translating resources.
     * @returns Internalization provider.
     */
    get i18n() {
        return (i18n ??= new I18nProvider(this.info.application.language, fileSystemLocaleProvider));
    },
    /**
     * Registration and application information provided by Stream Deck during initialization.
     * @returns Registration information.
     */
    get info() {
        return connection.registrationParameters.info;
    },
    /**
     * Logger responsible for capturing log messages.
     * @returns The logger.
     */
    get logger() {
        return logger$1;
    },
    /**
     * Namespace for Stream Deck profiles.
     * @returns Profiles namespace.
     */
    get profiles() {
        return profiles;
    },
    /**
     * Namespace for persisting settings within Stream Deck.
     * @returns Settings namespace.
     */
    get settings() {
        return settings;
    },
    /**
     * Namespace for interacting with, and receiving events from, the system the plugin is running on.
     * @returns System namespace.
     */
    get system() {
        return system;
    },
    /**
     * Namespace for interacting with UI (property inspector) associated with the plugin.
     * @returns UI namespace.
     */
    get ui() {
        return ui;
    },
    /**
     * Connects the plugin to the Stream Deck.
     * @returns A promise resolved when a connection has been established.
     */
    connect() {
        return connection.connect();
    },
};

var logger = streamDeck.logger.setLevel('info').createScope('Essentials for Spotify');

const API_EMPTY_RESPONSE = Symbol('API_EMPTY_RESPONSE');
const API_NOT_FOUND_RESPONSE = Symbol('API_NOT_FOUND_RESPONSE');

const WRAPPER_RESPONSE_SUCCESS = Symbol('WRAPPER_RESPONSE_SUCCESS');
const WRAPPER_RESPONSE_SUCCESS_INDICATIVE = Symbol('WRAPPER_RESPONSE_SUCCESS_INDICATIVE');
const WRAPPER_RESPONSE_API_RATE_LIMITED = Symbol('WRAPPER_RESPONSE_API_RATE_LIMITED');
const WRAPPER_RESPONSE_API_ERROR = Symbol('WRAPPER_RESPONSE_API_ERROR');
const WRAPPER_RESPONSE_FATAL_ERROR = Symbol('WRAPPER_RESPONSE_FATAL_ERROR');
const WRAPPER_RESPONSE_NO_DEVICE_ERROR = Symbol('WRAPPER_RESPONSE_NO_DEVICE_ERROR');
const WRAPPER_RESPONSE_BUSY = Symbol('WRAPPER_RESPONSE_BUSY');
const WRAPPER_RESPONSE_NOT_AVAILABLE = Symbol('WRAPPER_RESPONSE_NOT_AVAILABLE');
const WRAPPER_ITEMS_PER_PAGE = 50;

const BUTTON_MARQUEE_SPACING = 8;
const BUTTON_MARQUEE_SPACING_MULTIPLIER = 2;
const BUTTON_MARQUEE_INTERVAL = 350;
const BUTTON_MARQUEE_INTERVAL_INITIAL = 750;
const BUTTON_HOLD_DELAY = 500;
const BUTTON_HOLD_REPEAT_INTERVAL = 250;
const DIAL_MARQUEE_INTERVAL = 500;
const DIAL_MARQUEE_INTERVAL_INITIAL = 750;
const VOLUME_PERCENT_MUTE_RESTORE = 50;
const INTERVAL_CHECK_UPDATE_PLAYBACK_STATE = 500;
const SONG_CHANGE_FORCE_UPDATE_PLAYBACK_UNLOADED_SLEEP = 2500;
const SONG_CHANGE_FORCE_UPDATE_PLAYBACK_STATE_SLEEP = 1250;
const SONG_CHANGE_FORCE_UPDATE_PLAYBACK_TIME_SLEEP = 150;
const INTERVAL_UPDATE_PLAYBACK_STATE_PLAYING = 7500;
const INTERVAL_UPDATE_PLAYBACK_STATE_PAUSED = 15000;
const INTERVAL_UPDATE_PLAYBACK_STATE_IDLE = 30000;
const PLAYBACK_PAUSED_THRESHOLD = 15000;
const INTERVAL_KEEP_ALIVE_EXPECTED_CDN_IN_PLAYBACK_STATE = 30000;
const INTERVAL_CHECK_UPDATE_SONG_TIME = 500;
const BUTTON_MULTI_PRESS_INTERVAL = 250;
const CONNECTOR_DEFAULT_PORT = 6545;
const OVERLAY_DEFAULT_PORT = 6546;
const PORT_RETRY_RANGE = 512;
const LONG_FLASH_DURATION = 2000;
const LONG_FLASH_TIMES = 1;
const VERY_SHORT_FLASH_DURATION = 500;
const SHORT_FLASH_DURATION = 1000;
const SHORT_FLASH_TIMES = 1;
const DEFAULT_SEEK_STEP_SIZE = 5;
const DEFAULT_VOLUME_STEP = 5;
const DEFAULT_VOLUME_STACK_STEP = 10;

const CONNECTOR_DEFAULT_SCOPES = [
	'user-read-currently-playing',
	'user-read-playback-state',
	'user-modify-playback-state',
	'user-read-private',
	'user-library-read',
	'user-library-modify',
	'playlist-read-private',
	'playlist-modify-public',
	'playlist-modify-private',
	'playlist-read-collaborative'
];

const CHARACTER_WIDTH_MAP = {
	'A': 1,
	'B': 0.9,
	'C': 0.9,
	'D': 1,
	'E': 0.9,
	'F': 0.8,
	'G': 1,
	'H': 1,
	'I': 0.4,
	'J': 0.7,
	'K': 0.9,
	'L': 0.8,
	'M': 1.2,
	'N': 1,
	'O': 1,
	'P': 0.9,
	'Q': 1,
	'R': 0.9,
	'S': 0.9,
	'T': 0.9,
	'U': 1,
	'V': 1,
	'W': 1.3,
	'X': 0.9,
	'Y': 0.9,
	'Z': 0.9,
	'a': 0.9,
	'b': 0.9,
	'c': 0.8,
	'd': 0.9,
	'e': 0.9,
	'f': 0.5,
	'g': 0.9,
	'h': 0.9,
	'i': 0.4,
	'j': 0.5,
	'k': 0.8,
	'l': 0.4,
	'm': 1.4,
	'n': 0.9,
	'o': 0.9,
	'p': 0.9,
	'q': 0.9,
	'r': 0.6,
	's': 0.8,
	't': 0.6,
	'u': 0.9,
	'v': 0.8,
	'w': 1.2,
	'x': 0.8,
	'y': 0.8,
	'z': 0.8,
	' ': 0.5,
	'.': 0.4,
	',': 0.4,
	'!': 0.4,
	'?': 0.6,
	':': 0.4,
	';': 0.4,
	'-': 0.5,
	'_': 0.9,
	'+': 0.9,
	'=': 0.9,
	'/': 0.8,
	'\\': 0.8,
	'|': 0.4,
	'(': 0.5,
	')': 0.5,
	'[': 0.5,
	']': 0.5,
	'{': 0.6,
	'}': 0.6,
	'<': 0.9,
	'>': 0.9,
	'@': 1.3,
	'#': 1,
	'$': 0.9,
	'%': 1.3,
	'^': 0.7,
	'&': 1,
	'*': 0.6,
	'~': 0.7,
	'`': 0.4,
	'1': 0.7,
	'2': 0.7,
	'3': 0.7,
	'4': 0.7,
	'5': 0.7,
	'6': 0.7,
	'7': 0.7,
	'8': 0.7,
	'9': 0.7,
	'0': 0.7
};

const SENSITIVE_ENDPOINTS = [
	'me',
	'me/library',
	'me/tracks'
];

class ApiError extends Error {
	constructor(status, message) {
		super(message);
		this.status = status;
	}
}

class NoDeviceError extends Error {
	constructor(message) {
		super(message);
	}
}

var constants = {
	CHARACTER_WIDTH_MAP,
	CONNECTOR_DEFAULT_SCOPES,
	API_EMPTY_RESPONSE,
	API_NOT_FOUND_RESPONSE,
	VOLUME_PERCENT_MUTE_RESTORE,
	INTERVAL_CHECK_UPDATE_PLAYBACK_STATE,
	BUTTON_MARQUEE_INTERVAL,
	BUTTON_MARQUEE_SPACING,
	BUTTON_MARQUEE_INTERVAL_INITIAL,
	BUTTON_MARQUEE_SPACING_MULTIPLIER,
	INTERVAL_UPDATE_PLAYBACK_STATE_PLAYING,
	INTERVAL_UPDATE_PLAYBACK_STATE_PAUSED,
	INTERVAL_UPDATE_PLAYBACK_STATE_IDLE,
	PLAYBACK_PAUSED_THRESHOLD,
	INTERVAL_KEEP_ALIVE_EXPECTED_CDN_IN_PLAYBACK_STATE,
	SONG_CHANGE_FORCE_UPDATE_PLAYBACK_STATE_SLEEP,
	SONG_CHANGE_FORCE_UPDATE_PLAYBACK_TIME_SLEEP,
	SONG_CHANGE_FORCE_UPDATE_PLAYBACK_UNLOADED_SLEEP,
	CONNECTOR_DEFAULT_PORT,
	OVERLAY_DEFAULT_PORT,
	WRAPPER_RESPONSE_SUCCESS,
	WRAPPER_RESPONSE_API_ERROR,
	WRAPPER_RESPONSE_FATAL_ERROR,
	WRAPPER_RESPONSE_BUSY,
	BUTTON_HOLD_DELAY,
	BUTTON_HOLD_REPEAT_INTERVAL,
	LONG_FLASH_DURATION,
	LONG_FLASH_TIMES,
	VERY_SHORT_FLASH_DURATION,
	SHORT_FLASH_DURATION,
	SHORT_FLASH_TIMES,
	INTERVAL_CHECK_UPDATE_SONG_TIME,
	DEFAULT_SEEK_STEP_SIZE,
	DIAL_MARQUEE_INTERVAL,
	DIAL_MARQUEE_INTERVAL_INITIAL,
	WRAPPER_RESPONSE_NOT_AVAILABLE,
	WRAPPER_RESPONSE_SUCCESS_INDICATIVE,
	WRAPPER_RESPONSE_NO_DEVICE_ERROR,
	WRAPPER_RESPONSE_API_RATE_LIMITED,
	WRAPPER_ITEMS_PER_PAGE,
	BUTTON_MULTI_PRESS_INTERVAL,
	DEFAULT_VOLUME_STEP,
	DEFAULT_VOLUME_STACK_STEP,
	PORT_RETRY_RANGE,
	SENSITIVE_ENDPOINTS,
	ApiError,
	NoDeviceError
};

const MIME_TYPES = {
	'.html': 'text/html',
	'.css': 'text/css',
	'.js': 'application/javascript',
	'.json': 'application/json',
	'.png': 'image/png',
	'.svg': 'image/svg+xml',
	'.ico': 'image/x-icon'
};

function serveStatic(root) {
	const absRoot = path.resolve(root);

	return (pathname, res) => {
		if (pathname === '/' || pathname.endsWith('.html'))
			return false

		const filePath = path.join(absRoot, pathname);

		if (!filePath.startsWith(absRoot))
			return false

		try {
			const stat = fs.statSync(filePath);

			if (!stat.isFile())
				return false
		} catch {
			return false
		}

		const ext = path.extname(filePath);

		res.writeHead(200, {
			'Content-Type': MIME_TYPES[ext] || 'application/octet-stream'
		});

		fs.createReadStream(filePath).pipe(res);

		return true
	}
}

function sendFile(res, filePath) {
	const resolved = path.resolve(filePath);
	const ext = path.extname(resolved);

	res.writeHead(200, {
		'Content-Type': MIME_TYPES[ext] || 'application/octet-stream'
	});

	fs.createReadStream(resolved).pipe(res);
}

function sendJson(res, data) {
	const body = JSON.stringify(data);

	res.writeHead(200, {
		'Content-Type': 'application/json'
	});

	res.end(body);
}

function redirect(res, url) {
	res.writeHead(302, {
		'Location': url
	});

	res.end();
}

function send(res, status, text = '') {
	res.writeHead(status, {
		'Content-Type': 'text/plain'
	});

	res.end(text);
}

function parseQuery(url) {
	const parsed = new URL$1(url, 'http://127.0.0.1');
	return Object.fromEntries(parsed.searchParams)
}

function parsePath(url) {
	return new URL$1(url, 'http://127.0.0.1').pathname
}

function parseFormBody(req) {
	return new Promise((resolve, reject) => {
		let body = '';

		req.on('data', chunk => body += chunk);
		req.on('end', () => resolve(Object.fromEntries(new URLSearchParams(body))));
		req.on('error', reject);
	})
}

class Connector extends EventEmitter$1 {
	#staticHandler = serveStatic('./bin/setup')
	#accessToken = null
	#refreshToken = null
	#clientId = null
	#clientSecret = null
	#port = null
	#server = null
	#lastDeviceId = null
	#setup = false
	#faked = false
	#error = false
	#state = null

	#setSetup(state) {
		this.#setup = state;
		this.emit('setupStateChanged', state);
	}

	async #refreshAccessToken() {
		const response = await fetch('https://accounts.spotify.com/api/token', {
			method: 'POST',

			body: new URLSearchParams({
				refresh_token: this.#refreshToken,
				grant_type: 'refresh_token'
			}),

			headers: {
				'Content-Type': 'application/x-www-form-urlencoded',
				'Authorization': `Basic ${Buffer.from(`${this.#clientId}:${this.#clientSecret}`).toString('base64')}`
			}
		});

		if (response.status !== 200)
			throw new constants.ApiError(response.status, `The refresh token Spotify API call failed with status "${response.status}" and body "${await response.text()}".`)

		this.#accessToken = (await response.json()).access_token;

		logger.info('The access token has been refreshed.');
	}

	async callSpotifyApi(path, options = {}, allowResponses = []) {
		if (!this.#setup)
			throw new Error(`The Spotify API call "${path}" failed because the connector is not set up.`)

		let response = await fetch(`https://api.spotify.com/v1/${path}`, {
			...options,

			headers: {
				...options.headers,
				'Authorization': `Bearer ${this.#accessToken}`
			}
		});

		if (response.status === 401) {
			await this.#refreshAccessToken();

			response = await fetch(`https://api.spotify.com/v1/${path}`, {
				...options,

				headers: {
					...options.headers,
					'Authorization': `Bearer ${this.#accessToken}`
				}
			});
		}

		if (response.status === 204 && allowResponses.includes(constants.API_EMPTY_RESPONSE))
			return constants.API_EMPTY_RESPONSE
		else if (response.status === 404 && allowResponses.includes(constants.API_NOT_FOUND_RESPONSE))
			return constants.API_NOT_FOUND_RESPONSE

		if (response.status !== 200 && (response.status !== 201 || options.method !== 'POST'))
			throw new constants.ApiError(response.status, `The Spotify API call "${path}" with body ${JSON.stringify(options.body ?? {})} failed with status "${response.status}" and body "${await response.text()}".`)

		if (response.headers.get('content-type')?.includes('application/json'))
			return response.json()
		else
			return response.text()
	}

	async #handleRequest(req, res) {
		const pathname = parsePath(req.url);
		const query = parseQuery(req.url);

		if (this.#staticHandler(pathname, res))
			return
		else if (req.method === 'GET' && /^\/[a-z]{2}(_[A-Z]{2})?\.json$/.test(pathname))
			return sendFile(res, `.${pathname}`)
		else if (req.method === 'GET' && pathname === '/port')
			return send(res, 200, this.#port.toString())
		else if (req.method === 'POST' && pathname === '/') {
			const body = await parseFormBody(req);

			if ((!body.clientId) || (!body.clientSecret)) {
				this.#error = true;
				return redirect(res, '/?error=1')
			}

			this.#clientId = body.clientId;
			this.#clientSecret = body.clientSecret;
			this.#state = crypto.randomUUID();

			return redirect(res, `https://accounts.spotify.com/authorize?response_type=code&client_id=${this.#clientId}&scope=${encodeURIComponent(constants.CONNECTOR_DEFAULT_SCOPES.join(' '))}&redirect_uri=${encodeURIComponent(`http://127.0.0.1:${this.#port}`)}&state=${this.#state}`)
		} else if (req.method === 'GET' && pathname === '/')
			if (query.error)
				if (!this.#error)
					return redirect(res, '/')
				else {
					this.#error = false;
					return sendFile(res, './bin/setup/index.html')
				}
			else if (query.code && query.state === this.#state && (!this.#setup) && this.#clientId && this.#clientSecret)
				try {
					const response = await fetch('https://accounts.spotify.com/api/token', {
						method: 'POST',

						body: new URLSearchParams({
							code: query.code,
							redirect_uri: `http://127.0.0.1:${this.#port}`,
							grant_type: 'authorization_code'
						}),

						headers: {
							'Content-Type': 'application/x-www-form-urlencoded',
							'Authorization': `Basic ${Buffer.from(`${this.#clientId}:${this.#clientSecret}`).toString('base64')}`
						}
					});

					if (response.status !== 200)
						throw new constants.ApiError(response.status, `The access token Spotify API call failed with status "${response.status}".`)

					const data = await response.json();

					this.#refreshToken = data.refresh_token;
					this.#accessToken = data.access_token;
					this.#setSetup(true);

					this.#saveGlobalSettings({
						clientId: this.#clientId,
						clientSecret: this.#clientSecret,
						refreshToken: this.#refreshToken,
						accessToken: this.#accessToken
					});

					logger.info('The connector setup has been completed.');
					return redirect(res, '/?success=1')
				} catch (e) {
					logger.error(`An error occured while setting up the connector: "${e.message || 'No message.'}" @ "${e.stack || 'No stack trace.'}".`);
					this.#error = true;
					return redirect(res, '/?error=1')
				}
			else if (query.success && (!this.#setup))
				return redirect(res, '/')
			else {
				sendFile(res, './bin/setup/index.html');

				if (query.success && this.#setup) {
					this.#server.close();
					this.#server = null;
				}

				return
			}

		res.writeHead(404);
		res.end();
	}

	startSetup(clientId = null, clientSecret = null, refreshToken = null, lastDeviceId = null) {
		logger.info('Starting connector setup.');

		this.#clientId = clientId;
		this.#clientSecret = clientSecret;
		this.#refreshToken = refreshToken;
		this.#lastDeviceId = lastDeviceId;

		if (this.#refreshToken)
			this.#refreshAccessToken().then(() => this.#setSetup(true)).catch(e => {
				logger.error(`An error occured while setting up the connector: "${e.message || 'No message.'}" @ "${e.stack || 'No stack trace.'}".`);
				this.invalidateSetup(true);
			});
		else
			this.#listenWithRetry();
	}

	invalidateSetup(force = false) {
		if ((!force) && (!this.#setup))
			return

		this.#setSetup(false);

		this.#accessToken = null;
		this.#refreshToken = null;
		this.#clientId = null;
		this.#clientSecret = null;

		this.#listenWithRetry();

		this.#lastDeviceId = null;

		this.#saveGlobalSettings({
			clientId: null,
			clientSecret: null,
			refreshToken: null,
			accessToken: null,
			lastDeviceId: null
		});

		logger.warn('The connector setup has been invalidated.');
	}

	saveLastDeviceId(deviceId) {
		this.#lastDeviceId = deviceId;
		this.#saveGlobalSettings({ lastDeviceId: deviceId });
	}

	get lastDeviceId() {
		return this.#lastDeviceId
	}

	#listenWithRetry(attempt = 0) {
		const port = constants.CONNECTOR_DEFAULT_PORT + attempt;

		this.#server = http.createServer((req, res) => this.#handleRequest(req, res));

		this.#server.on('listening', () => {
			this.#port = port;

			logger.info(`Connector setup server listening on port "${port}".`);

			streamDeck.settings.getGlobalSettings().then(settings => {
				streamDeck.settings.setGlobalSettings({
					...settings,
					connectorPort: port
				});
			}).catch(e => logger.error(`An error occured while updating connector port in global settings: "${e.message || 'No message.'}".`));
		});

		this.#server.on('error', err => {
			if (err.code === 'EADDRINUSE' && attempt < constants.PORT_RETRY_RANGE) {
				logger.warn(`Port "${port}" in use, trying "${port + 1}".`);
				this.#listenWithRetry(attempt + 1);
			} else
				logger.error(`Failed to start connector setup server: "${err.message || 'No message.'}"`);
		});

		this.#server.listen(port);
	}

	#saveGlobalSettings(credentials) {
		streamDeck.settings.getGlobalSettings().then(settings => {
			streamDeck.settings.setGlobalSettings({
				...settings,
				...credentials
			});
		}).catch(e => logger.error(`An error occured while setting the Stream Deck global settings: "${e.message || 'No message.'}" @ "${e.stack || 'No stack trace.'}".`));
	}

	fakeOff() {
		this.#faked = true;
		this.#setup = false;
		this.emit('setupStateChanged', false);
	}

	fakeOn() {
		if (!this.#faked)
			return

		this.#faked = false;
		this.#setup = true;
		this.emit('setupStateChanged', true);
	}

	get set() {
		return this.#setup
	}

	get port() {
		return this.#port
	}
}

var connector = new Connector();

const expectedCdnDomains = new Set();

let lastSong = null;
let imageCache = {};
let pendingResults = {};

const getForSong = async song => {
	if (imageCache[`song:${song.item.id}`])
		return imageCache[`song:${song.item.id}`]

	if (pendingResults[`song:${song.item.id}`])
		return pendingResults[`song:${song.item.id}`]

	pendingResults[`song:${song.item.id}`] = new Promise(async (resolve, reject) => {
		try {
			if (imageCache[`song:${song.item.id}`]) {
				resolve(imageCache[`song:${song.item.id}`]);
				return
			}

			const url = song.item.album.images.length > 0 ? song.item.album.images.sort((a, b) => a.width - b.width)[0].url : undefined;

			if (!url) {
				resolve(null);
				return
			}

			const parsedUrl = new URL(url);

			if (!expectedCdnDomains.has(parsedUrl.hostname))
				expectedCdnDomains.add(parsedUrl.hostname);

			imageCache[`song:${song.item.id}`] = Buffer.from(await (await fetch(url)).arrayBuffer()).toString('base64');

			resolve(imageCache[`song:${song.item.id}`]);
		} catch (e) {
			logger.error(`Failed to get image for song "${song.item.id}": "${e.message}"`);
			resolve(null);
		}
	}).finally(result => {
		delete pendingResults[`song:${song.item.id}`];
		return result
	});

	return pendingResults[`song:${song.item.id}`]
};

const isSongCached = song => !!imageCache[`song:${song.item.id}`];

const getForItem = async item => {
	if (Object.keys(imageCache).length > constants.WRAPPER_ITEMS_PER_PAGE)
		imageCache = Object.keys(imageCache).reduce((acc, key) => {
			if (!key.startsWith('item:'))
				acc[key] = imageCache[key];

			return acc
		}, {});

	if (imageCache[`item:${item.id}`])
		return imageCache[`item:${item.id}`]

	if (pendingResults[`item:${item.id}`])
		return pendingResults[`item:${item.id}`]

	pendingResults[`item:${item.id}`] = new Promise(async (resolve, reject) => {
		try {
			if (imageCache[`item:${item.id}`]) {
				resolve(imageCache[`item:${item.id}`]);
				return
			}

			const url = item.images.length > 0 ? item.images.sort((a, b) => a.width - b.width)[0].url : undefined;

			if (!url) {
				resolve(null);
				return
			}

			const parsedUrl = new URL(url);

			if (!expectedCdnDomains.has(parsedUrl.hostname))
				expectedCdnDomains.add(parsedUrl.hostname);

			imageCache[`item:${item.id}`] = Buffer.from(await (await fetch(url)).arrayBuffer()).toString('base64');

			resolve(imageCache[`item:${item.id}`]);
		} catch (e) {
			logger.error(`Failed to get image for item "${item.id}": "${e.message}"`);
			resolve(null);
		}
	}).finally(result => {
		delete pendingResults[`item:${item.id}`];
		return result
	});

	return pendingResults[`item:${item.id}`]
};

const isItemCached = item => !!imageCache[`item:${item.id}`];

const getRaw = async (url, cacheKey) => {
	if (imageCache[cacheKey])
		return imageCache[cacheKey]

	if (pendingResults[cacheKey])
		return pendingResults[cacheKey]

	pendingResults[cacheKey] = new Promise(async (resolve, reject) => {
		try {
			if (imageCache[cacheKey]) {
				resolve(imageCache[cacheKey]);
				return
			}

			const parsedUrl = new URL(url);

			if (!expectedCdnDomains.has(parsedUrl.hostname))
				expectedCdnDomains.add(parsedUrl.hostname);

			imageCache[cacheKey] = Buffer.from(await (await fetch(url)).arrayBuffer()).toString('base64');

			resolve(imageCache[cacheKey]);
		} catch (e) {
			logger.error(`Failed to get image for URL "${url}": "${e.message}"`);
			resolve(null);
		}
	}).finally(result => {
		delete pendingResults[cacheKey];
		return result
	});

	return pendingResults[cacheKey]
};

const clearRaw = cacheKey => {
	delete imageCache[cacheKey];
};

const isRawCached = cacheKey => !!imageCache[cacheKey];

const onSongChanged = (song, pending) => {
	if (lastSong)
		delete imageCache[`song:${lastSong.item.id}`];

	if (song)
		lastSong = song;
};

var images = {
	getRaw,
	getForSong,
	getForItem,
	clearRaw,
	isSongCached,
	isItemCached,
	isRawCached,
	onSongChanged,
	expectedCdnDomains
};

class Wrapper extends EventEmitter$2 {
	#pendingWrappedCall = false
	#lastPlaying = null
	#lastMuted = null
	#lastShuffleState = null
	#lastPendingSong = null
	#lastPendingContext = null
	#lastVolumePercent = null
	#lastDeviceId = null
	#lastSong = null
	#previousSong = null
	#lastSongTimeUpdateAt = null
	#lastPlaybackContext = null
	#lastPlaybackStateUpdate = null
	#lastPausedAt = null
	#lastExpectedCdnKeepAlive = null
	#lastRepeatState = null
	#lastCurrentlyPlayingType = null
	#lastUser = null
	#songChangeForceUpdatePlaybackStateTimeout = null
	#lastDevices = []
	#lastDisallowFlags = []
	#updatePlaybackStateStatus = 'idle'
	#knownPlaylists = new Map()
	#oembedCache = new Map()
	#oembedPending = {}

	constructor() {
		super();

		connector.on('setupStateChanged', state => {
			if (state) {
				if (connector.lastDeviceId)
					this.#lastDeviceId = connector.lastDeviceId;

				this.#updatePlaybackState(true);
			} else {
				this.#updatePlaybackContext(null);
				this.#setPlaying(false);
				this.#setRepeatState('off');
				this.#setShuffleState(false);
				this.#setVolumePercent(null);
				this.#setSong(null);
				this.#setDevices(null, []);
				this.#setDisallowFlags([]);
				this.#setCurrentlyPlayingType(null);
				this.#setUser(null);
			}
		});

		if (connector.set) {
			if (connector.lastDeviceId)
				this.#lastDeviceId = connector.lastDeviceId;

			this.#updatePlaybackState();
		}

		setInterval(() => {
			if (!connector.set)
				return

			this.#updatePlaybackState();
		}, constants.INTERVAL_CHECK_UPDATE_PLAYBACK_STATE);

		setInterval(() => {
			if (!connector.set)
				return

			if ((!this.#lastSong) || (!this.#lastPlaying))
				return

			const timeDiff = (Date.now() - this.#lastSongTimeUpdateAt);

			this.#setSong({
				item: this.#lastSong.item,
				liked: this.#lastSong.liked,
				progress: Math.min(this.#lastSong.progress + timeDiff, this.#lastSong.item.duration_ms)
			}, false, true);
		}, constants.INTERVAL_CHECK_UPDATE_SONG_TIME);
	}

	async #wrapCall(fn, parallel = false) {
		if (!parallel) {
			if (this.#pendingWrappedCall)
				return constants.WRAPPER_RESPONSE_BUSY

			this.#updatePlaybackStateStatus = 'pause';
			this.#pendingWrappedCall = true;
		}

		try {
			return await fn()
		} catch (e) {
			let response = constants.WRAPPER_RESPONSE_FATAL_ERROR;

			if (e instanceof constants.ApiError)
				response = e.status == 429 ? constants.WRAPPER_RESPONSE_API_RATE_LIMITED : constants.WRAPPER_RESPONSE_API_ERROR;
			else if (e instanceof constants.NoDeviceError)
				return constants.WRAPPER_RESPONSE_NO_DEVICE_ERROR

			if (response !== constants.WRAPPER_RESPONSE_API_RATE_LIMITED)
				if (e.message.includes('Restriction violated'))
					return constants.WRAPPER_RESPONSE_NOT_AVAILABLE
				else
					logger.error(`An error occured while responding to a wrapper call: "${e.stack || e.message || 'No stack trace.'}".`);

			return response
		} finally {
			if (!parallel) {
				this.#updatePlaybackStateStatus = 'idle';
				this.#lastPlaybackStateUpdate = Date.now();
				this.#pendingWrappedCall = false;
			}
		}
	}

	async #deviceCall(path, options, deviceId, retried = false) {
		if (!deviceId) {
			if (this.#lastDeviceId)
				throw new constants.NoDeviceError('No device specified.')

			const devices = this.#lastDevices.filter(device => device.type !== 'Speaker');

			if (devices.length > 0)
				this.#setDevices(devices.find(device => device.is_active)?.id, devices);
		}

		path = `${path}${path.includes('?') ? '&' : '?'}`;

		let response = await connector.callSpotifyApi(`${path}${deviceId ? `device_id=${deviceId}` : ''}`, options, [constants.API_NOT_FOUND_RESPONSE, constants.API_EMPTY_RESPONSE]);

		if (response === constants.API_NOT_FOUND_RESPONSE) {
			const activeDevices = this.#lastDevices.filter(device => device.is_active);

			if (activeDevices.length > 0) {
				response = await connector.callSpotifyApi(`${path}device_id=${activeDevices[0].id}`, options, [constants.API_NOT_FOUND_RESPONSE, constants.API_EMPTY_RESPONSE]);

				if (response === constants.API_NOT_FOUND_RESPONSE)
					throw new constants.NoDeviceError('No device available.')
			} else if (!retried) {
				await new Promise(resolve => setTimeout(resolve, 3000));

				const freshDevices = ((await connector.callSpotifyApi('me/player/devices')).devices ?? []).filter(device => device.type !== 'Speaker');

				if (freshDevices.length > 0)
					this.#setDevices(freshDevices.find(device => device.id === this.#lastDeviceId)?.id ?? freshDevices.find(device => device.is_active)?.id, freshDevices);

				return this.#deviceCall(path.slice(0, -1), options, this.#lastDeviceId ?? deviceId, true)
			} else
				throw new constants.NoDeviceError('No device available.')
		}

		return response
	}

	#getPlaybackStateInterval() {
		if (!this.#lastSong)
			return constants.INTERVAL_UPDATE_PLAYBACK_STATE_IDLE
		else if (this.#lastPlaying)
			return constants.INTERVAL_UPDATE_PLAYBACK_STATE_PLAYING
		else if (this.#lastPausedAt && ((Date.now() - this.#lastPausedAt) >= constants.PLAYBACK_PAUSED_THRESHOLD))
			return constants.INTERVAL_UPDATE_PLAYBACK_STATE_PAUSED
		else
			return constants.INTERVAL_UPDATE_PLAYBACK_STATE_PLAYING
	}

	async #updatePlaybackState(force = false) {
		if (this.#updatePlaybackStateStatus === 'skip' && (!force)) {
			this.#updatePlaybackStateStatus = 'idle';
			return
		} else if (this.#updatePlaybackStateStatus === 'updating' || this.#updatePlaybackStateStatus === 'pause')
			return

		if (this.#lastPlaybackStateUpdate && (Date.now() - this.#lastPlaybackStateUpdate < this.#getPlaybackStateInterval()) && (!force))
			return

		const shouldKeepAliveExpectedCdn = this.#lastExpectedCdnKeepAlive && (Date.now() - this.#lastExpectedCdnKeepAlive < constants.INTERVAL_KEEP_ALIVE_EXPECTED_CDN_IN_PLAYBACK_STATE);

		if (shouldKeepAliveExpectedCdn)
			this.#lastExpectedCdnKeepAlive = Date.now();

		this.#lastPlaybackStateUpdate = Date.now();
		this.#updatePlaybackStateStatus = 'updating';

		try {
			if (shouldKeepAliveExpectedCdn)
				for (const domain of images.expectedCdnDomains)
					fetch(`https://${domain}/`, {
						method: 'HEAD'
					}).catch(() => {});

			let response = await connector.callSpotifyApi('me/player', undefined, [constants.API_EMPTY_RESPONSE]);

			if (response === constants.API_EMPTY_RESPONSE)
				response = undefined;

			if (!this.#lastUser)
				await this.updateUser();

			await this.#updatePlaybackContext(response?.context || null);

			this.#setPlaying(response?.is_playing || false);
			this.#setRepeatState(response?.repeat_state || 'off');
			this.#setShuffleState(response?.shuffle_state || false);
			this.#setVolumePercent(response?.device.supports_volume ? (typeof response?.device.volume_percent !== 'number' ? 100 : response.device.volume_percent) : null);

			this.#setSong(response?.item ? {
				item: response.item,
				liked: response.item.id && (this.#lastSong?.liked === undefined || response.item.id !== this.#lastSong?.item?.id) ? (await connector.callSpotifyApi(`me/library/contains?uris=${encodeURIComponent(response.item.uri)}`))[0] : this.#lastSong?.liked,
				progress: response.progress_ms
			} : null);

			this.#setDevices(response?.device.id ?? this.#lastDeviceId ?? null, (await connector.callSpotifyApi('me/player/devices')).devices ?? []);
			this.#setDisallowFlags((response?.actions?.disallows ? Object.keys(response.actions.disallows).filter(flag => response.actions.disallows[flag]) : []).concat(response?.device.supports_volume ? [] : 'volume'));
			this.#setCurrentlyPlayingType(response?.currently_playing_type || null);
		} catch (e) {
			logger.error(`An error occured while updating playback state: "${e.message || 'No message.'}" @ "${e.stack || 'No stack trace.'}".`);
		} finally {
			this.#updatePlaybackStateStatus = 'idle';
			this.#lastPlaybackStateUpdate = Date.now();
		}
	}

	async #getTypeData(type, uri, nullOnFailure = false) {
		let title = 'Unknown ❓';
		let subtitle = null;
		let extra = 'Unknown ❓';
		let images = [];

		const id = uri.split(':')[2];

		switch (type) {
			case 'track':
				const track = await this.#wrapCall(() => connector.callSpotifyApi(`tracks/${id}`), true);

				if ((!track) && nullOnFailure)
					return null

				title = track?.name ?? 'Unknown ❓';
				subtitle = track?.artists?.map(artist => artist.name).join(', ') ?? null;
				extra = 'Track 🎵';
				images = track?.album?.images ?? [];

				break
			case 'artist':
				const artist = await this.#wrapCall(() => connector.callSpotifyApi(`artists/${id}`), true);

				if ((!artist) && nullOnFailure)
					return null

				title = artist?.name ?? 'Unknown ❓';
				extra = 'Artist 👤';
				images = artist?.images ?? [];

				break

			case 'album':
				const album = await this.#wrapCall(() => connector.callSpotifyApi(`albums/${id}`), true);

				if ((!album) && nullOnFailure)
					return null

				switch (album.album_type) {
					case 'compilation':
						extra = 'Compilation 🗂️';
						break

					default:
						extra = 'Album 💿';
						break
				}

				title = album?.name ?? 'Unknown ❓';
				subtitle = album?.artists?.map(artist => artist.name).join(', ') ?? null;
				extra = 'Album 💿';
				images = album?.images ?? [];

				break

			case 'playlist':
				if (this.#knownPlaylists.has(id)) {
					title = this.#knownPlaylists.get(id);
					extra = 'Playlist 📃';
					break
				}

				const playlist = await this.#wrapCall(() => connector.callSpotifyApi(`playlists/${id}`, {}, [constants.API_NOT_FOUND_RESPONSE]), true);

				if ((!playlist) && nullOnFailure)
					return null

				title = playlist?.name ?? 'Unknown ❓';
				extra = 'Playlist 📃';
				images = playlist?.images ?? [];

				break

			case 'show':
				const show = await this.#wrapCall(() => connector.callSpotifyApi(`shows/${id}`), true);

				if ((!show) && nullOnFailure)
					return null

				title = show?.name ?? 'Unknown ❓';
				extra = 'Show 🎙️';
				images = show?.images ?? [];

				break

			case 'collection':
				title = uri.includes('user:') ? 'Liked Songs' : 'Unknown ❓';
				extra = 'Collection 📚';

				images = [{
					width: 64,
					height: 64,
					url: 'https://misc.scdn.co/liked-songs/liked-songs-64.jpg'
				}];

				break

			case 'local':
				title = 'Local Files 🖥️';
				extra = null;
				images = [];

				break
		}

		return {
			title,
			subtitle,
			extra,
			images
		}
	}

	async #updatePlaybackContext(context, pending = false) {
		const wasPending = this.#lastPendingContext;

		this.#lastPendingContext = pending;

		if ((!context) && this.#lastSong?.item.uri.includes('local:'))
			context = {
				type: 'local',
				uri: 'local'
			};

		if (this.#lastPlaybackContext?.uri === context?.uri) {
			if (wasPending && !pending)
				this.emit('playbackContextChanged', this.#lastPlaybackContext, pending);

			return
		}

		if (context) {
			const typeData = await this.#getTypeData(context.type, context.uri);

			context.id = `${context.uri?.split(':')[2]}`;
			context.images = typeData.images;
			context.title = typeData.title;
			context.subtitle = typeData.subtitle;
			context.extra = typeData.extra;
		}

		this.#updatePlaybackStateStatus = 'skip';
		this.#lastPlaybackContext = context;
		this.emit('playbackContextChanged', context, pending);
	}

	async #forwardSeekRaw(song, time, deviceId = this.#lastDeviceId) {
		if (this.#lastDisallowFlags.includes('seeking') || this.#lastDisallowFlags.includes('interrupting_playback') || this.#lastUser?.product !== 'premium')
			return constants.WRAPPER_RESPONSE_NOT_AVAILABLE

		return this.#wrapCall(async () => {
			await this.#deviceCall(`me/player/seek?position_ms=${song.progress + time}`, {
				method: 'PUT'
			}, deviceId);

			this.#setSong({
				item: song.item,
				liked: song.liked,
				progress: song.progress + time
			});

			return constants.WRAPPER_RESPONSE_SUCCESS
		})
	}

	async #backwardSeekRaw(song, time, deviceId = this.#lastDeviceId) {
		if (this.#lastDisallowFlags.includes('seeking') || this.#lastDisallowFlags.includes('interrupting_playback') || this.#lastUser?.product !== 'premium')
			return constants.WRAPPER_RESPONSE_NOT_AVAILABLE

		return this.#wrapCall(async () => {
			const newProgress = Math.max(0, song.progress - time);

			await this.#deviceCall(`me/player/seek?position_ms=${newProgress}`, {
				method: 'PUT'
			}, deviceId);

			this.#setSong({
				item: song.item,
				liked: song.liked,
				progress: newProgress
			});

			return constants.WRAPPER_RESPONSE_SUCCESS
		})
	}

	async #turnOnShuffle(deviceId = this.#lastDeviceId) {
		if (this.#lastDisallowFlags.includes('toggling_shuffle') || this.#lastUser?.product !== 'premium')
			return constants.WRAPPER_RESPONSE_NOT_AVAILABLE

		return this.#wrapCall(async () => {
			await this.#deviceCall('me/player/shuffle?state=true', {
				method: 'PUT'
			}, deviceId);

			this.#setShuffleState(true);

			return constants.WRAPPER_RESPONSE_SUCCESS
		})
	}

	async #turnOnContextRepeat(deviceId = this.#lastDeviceId) {
		if (this.#lastDisallowFlags.includes('toggling_repeat_context') || this.#lastUser?.product !== 'premium')
			return constants.WRAPPER_RESPONSE_NOT_AVAILABLE

		return this.#wrapCall(async () => {
			await this.#deviceCall('me/player/repeat?state=context', {
				method: 'PUT'
			}, deviceId);

			this.#setRepeatState('context');

			return constants.WRAPPER_RESPONSE_SUCCESS
		})
	}

	async #turnOnTrackRepeat(deviceId = this.#lastDeviceId) {
		if (this.#lastDisallowFlags.includes('toggling_repeat_track') || this.#lastUser?.product !== 'premium')
			return constants.WRAPPER_RESPONSE_NOT_AVAILABLE

		return this.#wrapCall(async () => {
			await this.#deviceCall('me/player/repeat?state=track', {
				method: 'PUT'
			}, deviceId);

			this.#setRepeatState('track');

			return constants.WRAPPER_RESPONSE_SUCCESS
		})
	}

	async #setPlaybackVolume(volumePercent, deviceId = this.#lastDeviceId) {
		if (this.#lastDisallowFlags.includes('volume') || (this.#lastDisallowFlags.includes('interrupting_playback') && volumePercent <= 0) || this.#lastUser?.product !== 'premium')
			return constants.WRAPPER_RESPONSE_NOT_AVAILABLE

		return this.#wrapCall(async () => {
			volumePercent = Math.max(0, Math.min(100, volumePercent));

			await this.#deviceCall(`me/player/volume?volume_percent=${volumePercent}`, {
				method: 'PUT'
			}, deviceId);

			this.#setVolumePercent(volumePercent);

			return constants.WRAPPER_RESPONSE_SUCCESS
		})
	}

	#setPlaying(playing) {
		if (this.#lastPlaying === playing)
			return

		if (playing) {
			this.#lastSongTimeUpdateAt = Date.now();
			this.#lastPausedAt = null;
		} else
			this.#lastPausedAt = Date.now();

		this.#updatePlaybackStateStatus = 'skip';
		this.#lastPlaying = playing;
		this.emit('playbackStateChanged', playing);
	}

	#setRepeatState(repeatState) {
		if (this.#lastRepeatState === repeatState)
			return

		this.#updatePlaybackStateStatus = 'skip';
		this.#lastRepeatState = repeatState;
		this.emit('repeatStateChanged', repeatState);
	}

	#setShuffleState(shuffleState) {
		if (this.#lastShuffleState === shuffleState)
			return

		this.#updatePlaybackStateStatus = 'skip';
		this.#lastShuffleState = shuffleState;
		this.emit('shuffleStateChanged', shuffleState);
	}

	#setVolumePercent(volumePercent) {
		if (this.#lastVolumePercent === volumePercent)
			return

		const previousVolumePercent = this.#lastVolumePercent;

		this.#updatePlaybackStateStatus = 'skip';
		this.#lastVolumePercent = volumePercent;
		this.emit('volumePercentChanged', volumePercent);

		if (volumePercent !== null)
			if (volumePercent > 0)
				this.#setMuted(false);
			else if (volumePercent === 0 && this.#lastMuted === false)
				this.#setMuted(previousVolumePercent);
	}

	#setMuted(volumePercent) {
		if (this.#lastMuted === volumePercent)
			return

		this.#updatePlaybackStateStatus = 'skip';

		if (typeof(volumePercent) !== 'number')
			this.#lastMuted = false;
		else
			this.#lastMuted = volumePercent;

		this.emit('mutedStateChanged', this.#lastMuted !== false);
	}

	#setSong(song, pending = false, allowPlaybackStateUpdate = false) {
		this.#lastPendingSong = pending;

		if (!allowPlaybackStateUpdate)
			this.#updatePlaybackStateStatus = 'skip';

		if (!song) {
			const hadSong = !!this.#lastSong;

			this.#lastSong = null;

			if (hadSong || this.#lastCurrentlyPlayingType !== 'track') {
				this.emit('songChanged', null, pending);

				images.onSongChanged(null, pending);
				
				this.emit('songLikedStateChanged', false, pending);
				this.emit('songTimeChanged', 0, 0, pending);

				if (this.#lastPlaybackContext?.type === 'local')
					this.#onContextChangeExpected();
			}
		} else {
			const previousSongChanged = this.#previousSong?.item?.id !== song.item.id;
			const previousSongLikedChanged = this.#previousSong?.liked !== song.liked;
			const previousSongTimeChanged = this.#previousSong?.progress !== song.progress || this.#previousSong?.item?.duration_ms !== song.item.duration_ms;

			if (this.#previousSong && (!previousSongChanged) && (!previousSongLikedChanged) && (!previousSongTimeChanged) && (this.#lastPlaying))
				return

			const songChanged = this.#lastSong?.item?.id !== song?.item?.id;
			const likedChanged = this.#lastSong?.liked !== song?.liked;
			const timeChanged = this.#lastSong?.progress !== song?.progress || this.#lastSong?.item?.duration_ms !== song?.duration_ms;

			this.#lastSong = song;
			this.#previousSong = JSON.parse(JSON.stringify(song));

			if (this.#lastPlaybackContext?.type === 'local' && (!song.item.uri.includes('local:')))
				this.#onContextChangeExpected();

			if (songChanged || timeChanged)
				this.#lastSongTimeUpdateAt = Date.now();

			if (songChanged) {
				this.emit('songChanged', song, pending);
				images.onSongChanged(song, pending);
			}

			if (likedChanged)
				this.emit('songLikedStateChanged', song.liked, pending);

			if (timeChanged)
				this.emit('songTimeChanged', song.progress, song.item.duration_ms, pending);

			if (this.#lastSong.progress >= this.#lastSong.item.duration_ms)
				this.#onSongChangeExpected(true);
		}
	}

	#setDevices(lastDeviceId, devices) {
		if (this.#lastDeviceId !== lastDeviceId) {
			this.#updatePlaybackStateStatus = 'skip';
			this.#lastDeviceId = lastDeviceId;
			this.emit('deviceChanged', lastDeviceId);

			if (lastDeviceId)
				connector.saveLastDeviceId(lastDeviceId);
		}

		if (this.#lastDevices && this.#lastDevices.length === devices.length && this.#lastDevices.every((device, index) => device.id === devices[index].id))
			return

		this.#updatePlaybackStateStatus = 'skip';
		this.#lastDevices = devices;
		this.emit('devicesChanged', devices);
	}

	#setDisallowFlags(disallowFlags) {
		if (this.#lastDisallowFlags.length === disallowFlags.length && this.#lastDisallowFlags.every((flag, index) => flag === disallowFlags[index]))
			return

		this.#updatePlaybackStateStatus = 'skip';
		this.#lastDisallowFlags = disallowFlags;
		this.emit('disallowFlagsChanged', disallowFlags);
	}

	#setCurrentlyPlayingType(type) {
		if (this.#lastCurrentlyPlayingType === type)
			return

		this.#updatePlaybackStateStatus = 'skip';
		this.#lastCurrentlyPlayingType = type;
		this.emit('currentlyPlayingTypeChanged', type);
	}

	#setUser(user) {
		this.#lastUser = user;
		this.emit('userChanged', user);
	}

	#onSongChangeExpected(byTime = false) {
		clearTimeout(this.#songChangeForceUpdatePlaybackStateTimeout);

		const wasSongLoaded = !!this.#lastSong;

		if (!wasSongLoaded)
			this.#onContextChangeExpected();

		this.#setSong(null, true);

		this.#songChangeForceUpdatePlaybackStateTimeout = setTimeout(() => this.#updatePlaybackState(true), wasSongLoaded ? (byTime ? constants.SONG_CHANGE_FORCE_UPDATE_PLAYBACK_TIME_SLEEP : constants.SONG_CHANGE_FORCE_UPDATE_PLAYBACK_STATE_SLEEP) : constants.SONG_CHANGE_FORCE_UPDATE_PLAYBACK_UNLOADED_SLEEP);
	}

	#onContextChangeExpected() {
		this.#updatePlaybackContext(null, true);
	}

	async transferPlayback(deviceId) {
		return this.#wrapCall(async () => {
			await connector.callSpotifyApi('me/player', {
				method: 'PUT',

				body: JSON.stringify({
					device_ids: [deviceId]
				})
			}, [constants.API_EMPTY_RESPONSE]);

			this.#setDevices(deviceId, this.#lastDevices);
			this.#setDisallowFlags(['volume', 'interrupting_playback', 'toggling_shuffle', 'toggling_repeat_context', 'toggling_repeat_track', 'seeking', 'skipping_next', 'skipping_prev']);
			this.#setVolumePercent(null);

			return constants.WRAPPER_RESPONSE_SUCCESS
		})
	}

	async updateUser() {
		return this.#wrapCall(async () => {
			let userResponse = await connector.callSpotifyApi('me');
			
			if ((!userResponse) || typeof userResponse !== 'object')
				userResponse = undefined;
			
			this.#setUser(userResponse || null);
		})
	}

	async resumePlayback(deviceId = this.#lastDeviceId) {
		if (this.#lastPlaying || this.#lastUser?.product !== 'premium')
			return constants.WRAPPER_RESPONSE_NOT_AVAILABLE

		return this.#wrapCall(async () => {
			await this.#deviceCall('me/player/play', {
				method: 'PUT'
			}, deviceId);

			if (!this.#lastSong)
				this.#onSongChangeExpected();

			this.#setPlaying(true);

			return constants.WRAPPER_RESPONSE_SUCCESS
		})
	}

	async pausePlayback(deviceId = this.#lastDeviceId) {
		if ((!this.#lastPlaying) || this.#lastDisallowFlags.includes('interrupting_playback') || this.#lastUser?.product !== 'premium')
			return constants.WRAPPER_RESPONSE_NOT_AVAILABLE

		return this.#wrapCall(async () => {
			await this.#deviceCall('me/player/pause', {
				method: 'PUT'
			}, deviceId);

			this.#setPlaying(false);

			return constants.WRAPPER_RESPONSE_SUCCESS
		})
	}

	async togglePlayback() {
		if (this.#lastPlaying)
			return this.pausePlayback()
		else
			return this.resumePlayback()
	}

	async toggleShuffle() {
		if (this.#lastShuffleState)
			return this.turnOffShuffle()
		else
			return this.#turnOnShuffle()
	}

	async toggleTrackRepeat() {
		if (this.#lastRepeatState === 'track')
			return this.turnOffRepeat()
		else
			return this.#turnOnTrackRepeat()
	}

	async toggleContextRepeat() {
		if (this.#lastRepeatState === 'context')
			return this.turnOffRepeat()
		else
			return this.#turnOnContextRepeat()
	}

	async volumeDown(step = constants.DEFAULT_VOLUME_STEP) {
		if (this.#lastMuted !== false && (this.#lastMuted || 0) > step)
			return this.unmuteVolume()
		else
			return this.#setPlaybackVolume(this.#lastVolumePercent - step)
	}

	async volumeUp(step = constants.DEFAULT_VOLUME_STEP) {
		return this.#setPlaybackVolume((this.#lastMuted !== false ? (this.#lastMuted || 0) : this.#lastVolumePercent) + (this.#lastMuted !== false ? 0 : step))
	}

	async setVolume(volumePercent) {
		return this.#setPlaybackVolume(volumePercent)
	}

	async nextSong(deviceId = this.#lastDeviceId) {
		if (this.#lastDisallowFlags.includes('skipping_next') || this.#lastDisallowFlags.includes('interrupting_playback') || this.#lastUser?.product !== 'premium')
			return constants.WRAPPER_RESPONSE_NOT_AVAILABLE

		return this.#wrapCall(async () => {
			await this.#deviceCall('me/player/next', {
				method: 'POST'
			}, deviceId);

			if (this.#lastCurrentlyPlayingType === 'track')
				this.#onSongChangeExpected();
			else
				await this.#updatePlaybackState(true);

			return constants.WRAPPER_RESPONSE_SUCCESS_INDICATIVE
		})
	}

	async previousSong(deviceId = this.#lastDeviceId) {
		if (this.#lastDisallowFlags.includes('skipping_prev') || this.#lastDisallowFlags.includes('interrupting_playback') || this.#lastUser?.product !== 'premium')
			return constants.WRAPPER_RESPONSE_NOT_AVAILABLE

		return this.#wrapCall(async () => {
			await this.#deviceCall('me/player/previous', {
				method: 'POST'
			}, deviceId);

			if (this.#lastCurrentlyPlayingType === 'track')
				this.#onSongChangeExpected();
			else
				await this.#updatePlaybackState(true);

			return constants.WRAPPER_RESPONSE_SUCCESS_INDICATIVE
		})
	}

	async turnOffShuffle(deviceId = this.#lastDeviceId) {
		if (this.#lastDisallowFlags.includes('toggling_shuffle') || this.#lastUser?.product !== 'premium')
			return constants.WRAPPER_RESPONSE_NOT_AVAILABLE

		return this.#wrapCall(async () => {
			await this.#deviceCall('me/player/shuffle?state=false', {
				method: 'PUT'
			}, deviceId);

			this.#setShuffleState(false);

			return constants.WRAPPER_RESPONSE_SUCCESS
		}, true)
	}

	async turnOffRepeat(deviceId = this.#lastDeviceId) {
		if ((this.#lastDisallowFlags.includes('toggling_repeat_context') && this.#lastRepeatState === 'context') || (this.#lastDisallowFlags.includes('toggling_repeat_track') && this.#lastRepeatState === 'track') || this.#lastUser?.product !== 'premium')
			return constants.WRAPPER_RESPONSE_NOT_AVAILABLE

		return this.#wrapCall(async () => {
			await this.#deviceCall('me/player/repeat?state=off', {
				method: 'PUT'
			}, deviceId);

			this.#setRepeatState('off');

			return constants.WRAPPER_RESPONSE_SUCCESS
		}, true)
	}

	async muteVolume(deviceId = this.#lastDeviceId) {
		const lastVolumePercent = this.#lastVolumePercent;

		return this.#setPlaybackVolume(0, deviceId).then(response => {
			if (response === constants.WRAPPER_RESPONSE_SUCCESS)
				this.#setMuted(lastVolumePercent);

			return response
		})
	}

	async unmuteVolume(deviceId = this.#lastDeviceId) {
		return this.#setPlaybackVolume(this.#lastMuted || constants.VOLUME_PERCENT_MUTE_RESTORE, deviceId)
	}

	async likeSong(song) {
		return this.#wrapCall(async () => {
			await connector.callSpotifyApi(`me/library?uris=${encodeURIComponent(song.item.uri)}`, {
				method: 'PUT'
			});

			this.#setSong({
				item: song.item,
				liked: true,
				progress: song.progress
			});

			return constants.WRAPPER_RESPONSE_SUCCESS
		})
	}

	async forwardSeek(step = constants.DEFAULT_SEEK_STEP_SIZE) {
		step = step * 1000;

		if (this.song)
			if (this.song.progress + step < this.song.item.duration_ms)
				return this.#forwardSeekRaw(this.song, step)
			else
				return constants.WRAPPER_RESPONSE_SUCCESS
		else if (this.pendingSongChange)
			return constants.WRAPPER_RESPONSE_BUSY
		else
			return constants.WRAPPER_RESPONSE_NOT_AVAILABLE
	}

	async backwardSeek(step = constants.DEFAULT_SEEK_STEP_SIZE) {
		step = step * 1000;

		if (this.song)
			return this.#backwardSeekRaw(this.song, step)
		else if (this.pendingSongChange)
			return constants.WRAPPER_RESPONSE_BUSY
		else
			return constants.WRAPPER_RESPONSE_NOT_AVAILABLE
	}

	async unlikeSong(song) {
		return this.#wrapCall(async () => {
			await connector.callSpotifyApi(`me/library?uris=${encodeURIComponent(song.item.uri)}`, {
				method: 'DELETE'
			});

			this.#setSong({
				item: song.item,
				liked: false,
				progress: song.progress
			});

			return constants.WRAPPER_RESPONSE_SUCCESS
		})
	}

	async toggleCurrentSongLike() {
		if (!this.#lastSong?.item.id)
			return constants.WRAPPER_RESPONSE_NOT_AVAILABLE
		else if (this.#lastSong.liked)
			return this.unlikeSong(this.#lastSong)
		else
			return this.likeSong(this.#lastSong)
	}

	async addSongToPlaylist(playlistId, trackUri) {
		return this.#wrapCall(async () => {
			await connector.callSpotifyApi(`playlists/${playlistId}/items`, {
				method: 'POST',

				body: JSON.stringify({
					uris: [trackUri]
				})
			});

			return constants.WRAPPER_RESPONSE_SUCCESS
		})
	}

	async toggleVolumeMute() {
		if (this.#lastVolumePercent === null)
			return constants.WRAPPER_RESPONSE_NOT_AVAILABLE
		else if (this.muted)
			return this.unmuteVolume()
		else
			return this.muteVolume()
	}

	async playItem(item, offset = undefined, deviceId = this.#lastDeviceId) {
		if (this.#lastUser?.product !== 'premium')
			return constants.WRAPPER_RESPONSE_NOT_AVAILABLE

		return this.#wrapCall(async () => {
			const contextUri = `spotify:${item.type}:${item.id}`;
			const songUri = offset?.uri;

			await this.#deviceCall('me/player/play', {
				method: 'PUT',

				body: JSON.stringify(item.type === 'track' ? {
					uris: [contextUri]
				} : {
					context_uri: contextUri,
					offset
				})
			}, deviceId);

			if (songUri && this.#lastSong?.item?.uri === songUri)
				this.#setSong({
					item: this.#lastSong.item,
					liked: this.#lastSong.liked,
					progress: 0
				});
			else
				this.#onSongChangeExpected();

			if (this.#lastPlaybackContext?.uri !== contextUri)
				this.#onContextChangeExpected();

			return constants.WRAPPER_RESPONSE_SUCCESS
		})
	}

	async getUserPlaylists(page = 1) {
		return this.#wrapCall(async () => {
			const playlists = await connector.callSpotifyApi(`me/playlists?limit=${constants.WRAPPER_ITEMS_PER_PAGE}&offset=${(page - 1) * constants.WRAPPER_ITEMS_PER_PAGE}`);

			return {
				status: constants.WRAPPER_RESPONSE_SUCCESS,

				items: playlists.items.map(playlist => ({
					id: playlist.id,
					type: 'playlist',
					name: playlist.name,
					images: playlist.images
				})),

				total: playlists.total
			}
		}, true)
	}

	async getUserLikedSongs(page = 1) {
		return this.#wrapCall(async () => {
			const tracks = await connector.callSpotifyApi(`me/tracks?limit=${constants.WRAPPER_ITEMS_PER_PAGE}&offset=${(page - 1) * constants.WRAPPER_ITEMS_PER_PAGE}`);

			return {
				status: constants.WRAPPER_RESPONSE_SUCCESS,

				items: tracks.items.map(item => {
					let extra = '';

					switch (item.track.album.album_type) {
						case 'album':
							extra = '💿';
							break

						case 'single':
							extra = '🎤';
							break
						
						case 'compilation':
							extra = '🗂️';
							break

						case 'ep':
							extra = '📀';
							break
					}

					return {
						id: item.track.id,
						type: 'track',
						extra,
						name: `${item.track.name} - ${item.track.artists.map(artist => artist.name).join(', ')}`,
						images: item.track.album.images
					}
				}),

				total: tracks.total
			}
		}, true)
	}

	async getCurrentTrack() {
		return this.#wrapCall(async () => {
			const response = await connector.callSpotifyApi('me/player/currently-playing', undefined, [constants.API_EMPTY_RESPONSE]);

			if (response === constants.API_EMPTY_RESPONSE || !response?.item)
				return null

			return {
				id: response.item.id,
				uri: response.item.uri,
				name: response.item.name
			}
		}, true)
	}

	async getInformationOnUrl(url) {
		try {
			const realUrl = new URL(url);

			realUrl.search = '';

			const parts = realUrl.pathname.split('/').filter(p => p);
			const startIndex = parts[0]?.match(/^intl-[a-z]{2}$/) ? 1 : 0;

			const type = parts[startIndex];
			const id = parts[startIndex + 1];

			if (!['artist', 'album', 'playlist', 'show', 'collection', 'local', 'track'].includes(type))
				return null

			let itemType = type;
			let itemId = id;
			let uri = `spotify:${type}:${id}`;

			if (type === 'collection')
				if (id === 'tracks') {
					if (!this.#lastUser?.id)
						return null

					itemType = 'user';
					itemId = `${this.#lastUser.id}:collection`;
					uri = `spotify:user:${this.#lastUser.id}:collection`;
				} else
					return null

			const typeData = await this.#getTypeData(type, uri, true);

			return typeData ? {
				url,
				uri,
				id: itemId,
				type: itemType,
				title: typeData.title,
				subtitle: typeData.subtitle,
				extra: typeData.extra,
				images: typeData.images
			} : null
		} catch {
			return null
		}
	}

	get playing() {
		return this.#lastPlaying
	}

	get repeatState() {
		return this.#lastRepeatState
	}

	get shuffleState() {
		return this.#lastShuffleState
	}

	get playbackContext() {
		return this.#lastPlaybackContext
	}

	get volumePercent() {
		return this.#lastVolumePercent
	}

	get mutedVolumePercent() {
		return this.#lastMuted || 0
	}

	get muted() {
		return this.#lastMuted !== false
	}

	get song() {
		return this.#lastSong
	}

	get device() {
		return this.#lastDeviceId
	}

	get user() {
		return this.#lastUser
	}

	get pendingSongChange() {
		return this.#lastPendingSong
	}

	get pendingContextChange() {
		return this.#lastPendingContext
	}

	get devices() {
		return this.#lastDevices
	}

	setKnownPlaylists(playlists) {
		this.#knownPlaylists.clear();

		for (const playlist of playlists)
			this.#knownPlaylists.set(playlist.id, playlist.name);
	}

	async getOembed(id) {
		if (this.#oembedCache.has(id)) {
			const value = this.#oembedCache.get(id);

			this.#oembedCache.delete(id);
			this.#oembedCache.set(id, value);

			return value
		} else if (this.#oembedPending[id])
			return this.#oembedPending[id]

		this.#oembedPending[id] = new Promise(async (resolve) => {
			try {
				if (this.#oembedCache.has(id)) {
					resolve(this.#oembedCache.get(id));
					return
				}

				const response = await fetch(`https://open.spotify.com/oembed?url=https://open.spotify.com/playlist/${id}`);
				const data = await response.json();

				const result = {
					thumbnailUrl: data.thumbnail_url || null,
					title: data.title || null
				};

				if (this.#oembedCache.size >= constants.WRAPPER_ITEMS_PER_PAGE)
					this.#oembedCache.delete(this.#oembedCache.keys().next().value);

				this.#oembedCache.set(id, result);

				resolve(result);
			} catch (e) {
				logger.error(`Failed to get oEmbed data for playlist "${id}": "${e.message}"`);
				resolve(null);
			}
		}).finally(result => {
			delete this.#oembedPending[id];
			return result
		});

		return this.#oembedPending[id]
	}
}

var wrapper = new Wrapper();

class OverlayServer {
	#staticHandler = serveStatic('./bin/overlay')
	#server = null
	#wss = null
	#port = null
	#subscribed = false

	start() {
		if (!this.#subscribed) {
			this.#subscribeToEvents();
			this.#subscribed = true;
		}

		this.#listenWithRetry();
	}

	#handleRequest(req, res) {
		const url = new URL(req.url, 'http://localhost');

		if (this.#staticHandler(url.pathname, res))
			return
		else if (req.method === 'GET' && url.pathname === '/')
			return sendFile(res, './bin/overlay/index.html')
		else if (req.method === 'GET' && url.pathname === '/status')
			return sendJson(res, {
				connected: connector.set,
				clients: this.#wss?.clients?.size ?? 0,
				port: this.#port
			})

		res.writeHead(404);
		res.end();
	}

	#listenWithRetry(attempt = 0) {
		const port = constants.OVERLAY_DEFAULT_PORT + attempt;

		this.#server = http.createServer((req, res) => this.#handleRequest(req, res));

		this.#wss = new WebSocketServer({
			server: this.#server
		});

		this.#wss.on('connection', ws => {
			logger.info('Overlay server: new WebSocket client connected.');

			ws.send(JSON.stringify(this.#buildFullState()));

			ws.on('error', (err) => logger.error(`Overlay server WebSocket error: "${err.message}"`));
		});

		this.#server.on('error', err => {
			if (err.code === 'EADDRINUSE' && attempt < constants.PORT_RETRY_RANGE) {
				logger.warn(`Port "${port}" in use, trying "${port + 1}".`);
				this.#listenWithRetry(attempt + 1);
			} else
				logger.error(`Failed to start overlay server: "${err.message || 'No message.'}"`);
		});

		this.#server.listen(port, () => {
			this.#port = port;
			logger.info(`Overlay server listening on port "${port}".`);

			streamDeck.settings.getGlobalSettings().then(settings => {
				streamDeck.settings.setGlobalSettings({
					...settings,
					overlayPort: port
				});
			}).catch(e => logger.error(`An error occured while updating overlay port in global settings: "${e.message || 'No message.'}".`));
		});
	}

	#subscribeToEvents() {
		connector.on('setupStateChanged', state => this.#broadcast({
			type: 'setupStateChanged',
			connected: state
		}));

		wrapper.on('songChanged', (song, pending) => this.#broadcast({
			type: 'songChanged',
			song: song ? this.#sanitizeSong(song) : null,
			pending
		}));

		wrapper.on('songTimeChanged', (progress, duration, pending) => this.#broadcast({
			type: 'songTimeChanged',
			progress,
			duration,
			pending
		}));

		wrapper.on('playbackStateChanged', playing => this.#broadcast({
			type: 'playbackStateChanged',
			playing
		}));

		wrapper.on('songLikedStateChanged', (liked, pending) => this.#broadcast({
			type: 'songLikedStateChanged',
			liked,
			pending
		}));

		wrapper.on('shuffleStateChanged', state => this.#broadcast({
			type: 'shuffleStateChanged',
			shuffle: state
		}));

		wrapper.on('repeatStateChanged', state => this.#broadcast({
			type: 'repeatStateChanged',
			repeat: state
		}));

		wrapper.on('volumePercentChanged', percent => this.#broadcast({
			type: 'volumePercentChanged',
			volume: percent
		}));

		wrapper.on('playbackContextChanged', context => this.#broadcast({
			type: 'playbackContextChanged',

			context: context ? {
				type: context.type,
				title: context.title,
				subtitle: context.subtitle,
				uri: context.uri
			} : null
		}));
	}

	#buildFullState() {
		const song = wrapper.song;

		return {
			type: 'fullState',
			connected: connector.set,
			playing: wrapper.playing ?? false,
			song: song ? this.#sanitizeSong(song) : null,
			repeat: wrapper.repeatState ?? 'off',
			shuffle: wrapper.shuffleState ?? false,
			volume: wrapper.volumePercent,

			context: wrapper.playbackContext ? {
				type: wrapper.playbackContext.type,
				title: wrapper.playbackContext.title,
				subtitle: wrapper.playbackContext.subtitle,
				uri: wrapper.playbackContext.uri
			} : null
		}
	}

	#sanitizeSong(song) {
		return {
			name: song.item?.name ?? null,
			artists: song.item?.artists?.map(a => a.name) ?? [],
			albumArt: song.item?.album?.images?.length > 0 ? song.item.album.images.sort((a, b) => b.width - a.width)[0].url : null,
			duration: song.item?.duration_ms ?? 0,
			progress: song.progress ?? 0,
			liked: song.liked ?? false,
			explicit: song.item?.explicit ?? false
		}
	}

	#broadcast(message) {
		if (!this.#wss)
			return

		const data = JSON.stringify(message);

		for (const client of this.#wss.clients)
			if (client.readyState === 1)
				client.send(data);
	}
}

var overlayServer = new OverlayServer();

/******************************************************************************
Copyright (c) Microsoft Corporation.

Permission to use, copy, modify, and/or distribute this software for any
purpose with or without fee is hereby granted.

THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES WITH
REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY
AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT,
INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM
LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR
OTHER TORTIOUS ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR
PERFORMANCE OF THIS SOFTWARE.
***************************************************************************** */
/* global Reflect, Promise, SuppressedError, Symbol, Iterator */


function __esDecorate(ctor, descriptorIn, decorators, contextIn, initializers, extraInitializers) {
    function accept(f) { if (f !== void 0 && typeof f !== "function") throw new TypeError("Function expected"); return f; }
    var kind = contextIn.kind, key = kind === "getter" ? "get" : kind === "setter" ? "set" : "value";
    var target = !descriptorIn && ctor ? contextIn["static"] ? ctor : ctor.prototype : null;
    var descriptor = descriptorIn || (target ? Object.getOwnPropertyDescriptor(target, contextIn.name) : {});
    var _, done = false;
    for (var i = decorators.length - 1; i >= 0; i--) {
        var context = {};
        for (var p in contextIn) context[p] = p === "access" ? {} : contextIn[p];
        for (var p in contextIn.access) context.access[p] = contextIn.access[p];
        context.addInitializer = function (f) { if (done) throw new TypeError("Cannot add initializers after decoration has completed"); extraInitializers.push(accept(f || null)); };
        var result = (0, decorators[i])(kind === "accessor" ? { get: descriptor.get, set: descriptor.set } : descriptor[key], context);
        if (kind === "accessor") {
            if (result === void 0) continue;
            if (result === null || typeof result !== "object") throw new TypeError("Object expected");
            if (_ = accept(result.get)) descriptor.get = _;
            if (_ = accept(result.set)) descriptor.set = _;
            if (_ = accept(result.init)) initializers.unshift(_);
        }
        else if (_ = accept(result)) {
            if (kind === "field") initializers.unshift(_);
            else descriptor[key] = _;
        }
    }
    if (target) Object.defineProperty(target, contextIn.name, descriptor);
    done = true;
}
function __runInitializers(thisArg, initializers, value) {
    var useValue = arguments.length > 2;
    for (var i = 0; i < initializers.length; i++) {
        value = useValue ? initializers[i].call(thisArg, value) : initializers[i].call(thisArg);
    }
    return useValue ? value : void 0;
}
typeof SuppressedError === "function" ? SuppressedError : function (error, suppressed, message) {
    var e = new Error(message);
    return e.name = "SuppressedError", e.error = error, e.suppressed = suppressed, e;
};

class Action extends SingletonAction {
    settings = {};
    contexts = [];
    actionObjects = new Map();
    constructor() {
        super();
        connector.on('setupStateChanged', (state) => {
            if (state)
                for (const context of this.contexts)
                    this.onSettingsUpdated(context, this.settings[context]);
        });
    }
    async onWillAppear(ev) {
        await super.onWillAppear?.(ev);
        this.contexts.push(ev.action.id);
        this.actionObjects.set(ev.action.id, ev.action);
        const oldSettings = JSON.parse(JSON.stringify(this.settings[ev.action.id] || {}));
        this.settings[ev.action.id] = ev.payload.settings;
        if (connector.set)
            await this.onSettingsUpdated(ev.action.id, oldSettings);
    }
    async onWillDisappear(ev) {
        this.contexts.splice(this.contexts.indexOf(ev.action.id), 1);
        this.actionObjects.delete(ev.action.id);
    }
    async onDidReceiveSettings(ev) {
        const oldSettings = JSON.parse(JSON.stringify(this.settings[ev.action.id] || {}));
        this.settings[ev.action.id] = ev.payload.settings;
        if (connector.set)
            await this.onSettingsUpdated(ev.action.id, oldSettings);
    }
    async setSettings(context, settings, internal = true) {
        const oldSettings = JSON.parse(JSON.stringify(this.settings[context] || {}));
        Object.assign(this.settings[context], settings);
        await this.actionObjects.get(context)?.setSettings(this.settings[context]).catch((e) => logger.error(`An error occurred while setting the Stream Deck settings of "${this.manifestId}": "${e.message || 'No message.'}" @ "${e.stack || 'No stack trace.'}".`));
        if ((!internal) && connector.set)
            await this.onSettingsUpdated(context, oldSettings);
    }
    async onSettingsUpdated(context, oldSettings) { }
    splitToLines(text, maxLineLength = 9, maxLines = 5) {
        const ELLIPSIS = '...';
        const truncateWithEllipsis = (s, limit) => {
            if (limit <= 0)
                return '';
            if (s.length <= limit)
                return s;
            if (limit <= ELLIPSIS.length)
                return s.slice(0, limit);
            return s.slice(0, limit - ELLIPSIS.length) + ELLIPSIS;
        };
        const tokens = text.trim().split(/[ \t]+|[-–—_\/\\.:|]+/).filter(Boolean);
        const lines = [];
        for (let i = 0; i < tokens.length; i++) {
            if (lines.length < maxLines - 1) {
                lines.push(truncateWithEllipsis(tokens[i], maxLineLength));
                continue;
            }
            if (lines.length === maxLines - 1)
                if (i < tokens.length - 1) {
                    lines.push(truncateWithEllipsis(ELLIPSIS, maxLineLength));
                    break;
                }
                else {
                    lines.push(truncateWithEllipsis(tokens[i], maxLineLength));
                    break;
                }
            break;
        }
        return lines.join('\n');
    }
    processImage(iconDataUrl, heart, borderColor = null) {
        if ((heart === 'none' && (!borderColor)))
            return iconDataUrl;
        const iconSize = 120;
        const heartPadding = 24;
        const originalMinX = 7;
        const originalMinY = 12;
        const originalWidth = 116;
        const originalHeight = 106;
        const availableWidth = iconSize - (heartPadding * 2);
        const availableHeight = iconSize - (heartPadding * 2);
        const borderStrokeWidth = borderColor ? 12 : 0;
        const borderInset = borderColor ? borderStrokeWidth / 2 : 0;
        const glowRadius = borderColor ? Math.max(4, Math.round(borderStrokeWidth * 1.25)) : 0;
        const borderRect = borderColor ? `
			<rect x="${borderInset}" y="${borderInset}" width="${iconSize - (borderInset * 2)}" height="${iconSize - (borderInset * 2)}" fill="none" stroke="${borderColor}" stroke-width="${borderStrokeWidth}" shape-rendering="geometricPrecision"/>
		` : '';
        let scaleFactor = Math.min(availableWidth / originalWidth, availableHeight / originalHeight);
        let heartWidth = originalWidth * scaleFactor;
        let heartHeight = originalHeight * scaleFactor;
        let offsetX = (iconSize - heartWidth) / 2;
        let offsetY = (iconSize - heartHeight) / 2;
        let strokeWidth = 2;
        if (heart === 'top-left') {
            const cornerPadding = 12;
            const maxCornerSize = iconSize * 0.4;
            scaleFactor = Math.min(maxCornerSize / originalWidth, maxCornerSize / originalHeight);
            heartWidth = originalWidth * scaleFactor;
            heartHeight = originalHeight * scaleFactor;
            offsetX = cornerPadding;
            offsetY = cornerPadding;
            strokeWidth = 1;
        }
        const heartShape = heart !== 'none' ? `
			<path d="M 65,29 C 59,19 49,12 37,12 20,12 7,25 7,42 7,75 25,80 65,118 105,80 123,75 123,42 123,25 110,12 93,12 81,12 71,19 65,29 z"
				fill="#1db954" stroke="#191414" stroke-width="${strokeWidth}" stroke-linecap="round" stroke-linejoin="round"
				transform="translate(${offsetX - (originalMinX * scaleFactor)}, ${offsetY - (originalMinY * scaleFactor)}) scale(${scaleFactor})"
				vector-effect="non-scaling-stroke"/>
		` : '';
        const innerGlowRect = borderColor ? `
			<rect x="${borderInset}" y="${borderInset}" width="${iconSize - (borderInset * 2)}" height="${iconSize - (borderInset * 2)}" fill="white" filter="url(#innerGlow)"/>
		` : '';
        const svg = `
			<svg width="${iconSize}" height="${iconSize}" viewBox="0 0 ${iconSize} ${iconSize}" xmlns="http://www.w3.org/2000/svg">

			<defs>
				<pattern id="iconPattern" patternUnits="userSpaceOnUse" width="${iconSize}" height="${iconSize}">
					<image href="${iconDataUrl}" x="0" y="0" width="${iconSize}" height="${iconSize}"/>
				</pattern>

				<filter id="innerGlow" x="-50%" y="-50%" width="200%" height="200%" color-interpolation-filters="sRGB">
					<feGaussianBlur in="SourceAlpha" stdDeviation="${glowRadius}" result="blur"/>
					<feComposite in="SourceAlpha" in2="blur" operator="out" result="innerEdge"/>
					<feFlood flood-color="#${borderColor}" flood-opacity="1" result="glowColor"/>
					<feComposite in="glowColor" in2="innerEdge" operator="in" result="coloredGlow"/>
					<feComposite in="coloredGlow" in2="coloredGlow" operator="over"/>
				</filter>
			</defs>

			<rect width="${iconSize}" height="${iconSize}" fill="url(#iconPattern)"/>

			${innerGlowRect}
			${borderRect}
			${heartShape}
			</svg>
		`;
        return `data:image/svg+xml;base64,${Buffer.from(svg).toString('base64')}`;
    }
    beautifyTime(progressMs, durationMs, showProgress = true, showDuration = true, remaining = false) {
        const progress = Math.floor(progressMs / 1000);
        const duration = Math.floor(durationMs / 1000);
        const progressMinutes = Math.floor(progress / 60);
        const progressSeconds = progress - (progressMinutes * 60);
        const secondValue = remaining ? Math.max(0, duration - progress) : duration;
        const secondMinutes = Math.floor(secondValue / 60);
        const secondSeconds = secondValue - (secondMinutes * 60);
        return `${showProgress ? `${progressMinutes}:${progressSeconds.toString().padStart(2, '0')}` : ''}${showProgress && showDuration ? ' / ' : ''}${showDuration ? `${remaining ? '-' : ''}${secondMinutes}:${secondSeconds.toString().padStart(2, '0')}` : ''}`;
    }
}

var _a$1;
class Button extends Action {
    static ACTIONLESS = false;
    static HOLDABLE = false;
    static MULTI = false;
    static SETUPLESS = false;
    static STATABLE = false;
    static TYPES = {
        SINGLE_PRESS: Symbol('SINGLE_PRESS'),
        DOUBLE_PRESS: Symbol('DOUBLE_PRESS'),
        TRIPLE_PRESS: Symbol('TRIPLE_PRESS'),
        LONG_PRESS: Symbol('LONG_PRESS'),
        HOLDING: Symbol('HOLDING'),
        RELEASED: Symbol('RELEASED')
    };
    #pressed = {};
    #holding = {};
    #busy = {};
    #unpressable = {};
    #flashing = {};
    #statelessImage = '';
    #keyUpTracker = {};
    #lastImage = {};
    #lastTitle = {};
    marquees = {};
    constructor() {
        super();
        if (this.constructor.STATABLE)
            connector.on('setupStateChanged', (state) => {
                if (!state)
                    for (const context of this.contexts)
                        this.onStateLoss(context);
                else
                    for (const context of this.contexts)
                        this.onStateSettled(context);
            });
    }
    async #flashImage(action, image, duration = 500, times = 2) {
        action.setTitle('');
        this.#flashing[action.id] = new Promise(async (resolve) => {
            this.pauseMarquee(action.id);
            for (let i = 0; i < times; i++) {
                await action.setImage(image).catch((e) => logger.error(`An error occurred while setting the Stream Deck image of "${this.manifestId}": "${e.message || 'No message.'}" @ "${e.stack || 'No stack trace.'}".`));
                await new Promise(resolve => setTimeout(resolve, duration));
                await action.setImage(this.#lastImage[action.id] ?? (this.constructor.STATABLE && (!connector.set) ? this.#statelessImage : undefined)).catch((e) => logger.error(`An error occurred while setting the Stream Deck image of "${this.manifestId}": "${e.message || 'No message.'}" @ "${e.stack || 'No stack trace.'}".`));
                if (i + 1 < times)
                    await new Promise(resolve => setTimeout(resolve, duration));
            }
            if (this.#lastTitle[action.id])
                await action.setTitle(this.#lastTitle[action.id]);
            delete this.#flashing[action.id];
            if (this.constructor.STATABLE && connector.set)
                await this.onStateSettled(action.id);
            this.resumeMarquee(action.id, true);
            resolve(true);
        });
    }
    async onKeyDown(ev) {
        if (this.#busy[ev.action.id] || this.#unpressable[ev.action.id] || this.constructor.ACTIONLESS)
            return;
        this.#busy[ev.action.id] = true;
        this.#pressed[ev.action.id] = {
            invoked: false,
            timeout: null,
            long: false
        };
        if ((!this.#holding[ev.action.id]) && this.constructor.HOLDABLE) {
            this.#holding[ev.action.id] = {
                held: false,
                resolve: null,
                timeout: null
            };
            await new Promise(resolve => {
                this.#holding[ev.action.id].resolve = resolve;
                this.#holding[ev.action.id].timeout = setTimeout(() => {
                    if (this.#pressed[ev.action.id] && this.#holding[ev.action.id])
                        this.#holding[ev.action.id].held = true;
                    resolve(true);
                }, constants.BUTTON_HOLD_DELAY);
            });
            if (!this.#holding[ev.action.id])
                return;
        }
        else if ((!this.constructor.HOLDABLE) && this.constructor.MULTI)
            this.#pressed[ev.action.id].timeout = setTimeout(async () => {
                if (this.#pressed[ev.action.id]) {
                    this.#pressed[ev.action.id].long = true;
                    this.#pressed[ev.action.id].invoked = true;
                    await this.#invokePress(ev, true, 1);
                }
            }, constants.BUTTON_HOLD_DELAY);
        if (this.#holding[ev.action.id] || (!this.constructor.MULTI))
            if ((!connector.set) && (!this.constructor.SETUPLESS))
                await this.#flashImage(ev.action, 'images/states/setup-error', constants.LONG_FLASH_DURATION, constants.LONG_FLASH_TIMES);
            else {
                this.#pressed[ev.action.id].invoked = true;
                const held = this.#holding[ev.action.id]?.held || false;
                const startedInvokingAt = Date.now();
                const response = await this.invokeWrapperAction(ev.action.id, this.#holding[ev.action.id] ? _a$1.TYPES.HOLDING : _a$1.TYPES.SINGLE_PRESS);
                if (response === constants.WRAPPER_RESPONSE_SUCCESS_INDICATIVE)
                    await this.#flashImage(ev.action, 'images/states/success', constants.SHORT_FLASH_DURATION, constants.SHORT_FLASH_TIMES);
                if ((response === constants.WRAPPER_RESPONSE_SUCCESS || response === constants.WRAPPER_RESPONSE_SUCCESS_INDICATIVE || response === constants.WRAPPER_RESPONSE_BUSY) && (held || this.#holding[ev.action.id]?.held) && this.#pressed[ev.action.id])
                    this.#pressed[ev.action.id].timeout = setTimeout(() => {
                        if (this.#pressed[ev.action.id])
                            this.onKeyDown(ev);
                    }, Math.max(0, constants.BUTTON_HOLD_REPEAT_INTERVAL - (Date.now() - startedInvokingAt)));
                else if (response === constants.WRAPPER_RESPONSE_NOT_AVAILABLE)
                    await this.#flashImage(ev.action, 'images/states/not-available', constants.LONG_FLASH_DURATION, constants.LONG_FLASH_TIMES);
                else if (response === constants.WRAPPER_RESPONSE_API_RATE_LIMITED)
                    await this.#flashImage(ev.action, 'images/states/api-rate-limited', constants.LONG_FLASH_DURATION, constants.LONG_FLASH_TIMES);
                else if (response === constants.WRAPPER_RESPONSE_API_ERROR)
                    await this.#flashImage(ev.action, 'images/states/api-error', constants.LONG_FLASH_DURATION, constants.LONG_FLASH_TIMES);
                else if (response === constants.WRAPPER_RESPONSE_FATAL_ERROR)
                    await this.#flashImage(ev.action, 'images/states/fatal-error', constants.LONG_FLASH_DURATION, constants.LONG_FLASH_TIMES);
                else if (response === constants.WRAPPER_RESPONSE_NO_DEVICE_ERROR)
                    await this.#flashImage(ev.action, 'images/states/no-device-error', constants.LONG_FLASH_DURATION, constants.LONG_FLASH_TIMES);
                else if (response === constants.WRAPPER_RESPONSE_BUSY)
                    await this.#flashImage(ev.action, 'images/states/busy', constants.SHORT_FLASH_DURATION, constants.SHORT_FLASH_TIMES);
            }
        if ((!this.#holding[ev.action.id]) && this.#pressed[ev.action.id]?.invoked) {
            this.#pressed[ev.action.id].invoked = false;
            await this.invokeWrapperAction(ev.action.id, _a$1.TYPES.RELEASED);
        }
        delete this.#busy[ev.action.id];
    }
    async #invokePress(ev, long, presses) {
        if ((!connector.set) && (!this.constructor.SETUPLESS))
            await this.#flashImage(ev.action, 'images/states/setup-error', constants.LONG_FLASH_DURATION, constants.LONG_FLASH_TIMES);
        else {
            if (long)
                await this.#flashImage(ev.action, 'images/states/long', constants.VERY_SHORT_FLASH_DURATION, constants.SHORT_FLASH_TIMES);
            const response = await this.invokeWrapperAction(ev.action.id, long ? _a$1.TYPES.LONG_PRESS : (presses === 1 ? _a$1.TYPES.SINGLE_PRESS : (presses === 2 ? _a$1.TYPES.DOUBLE_PRESS : _a$1.TYPES.TRIPLE_PRESS)));
            if (response === constants.WRAPPER_RESPONSE_SUCCESS_INDICATIVE)
                await this.#flashImage(ev.action, 'images/states/success', constants.SHORT_FLASH_DURATION, constants.SHORT_FLASH_TIMES);
            else if (response === constants.WRAPPER_RESPONSE_NOT_AVAILABLE)
                await this.#flashImage(ev.action, 'images/states/not-available', constants.LONG_FLASH_DURATION, constants.LONG_FLASH_TIMES);
            else if (response === constants.WRAPPER_RESPONSE_API_RATE_LIMITED)
                await this.#flashImage(ev.action, 'images/states/api-rate-limited', constants.LONG_FLASH_DURATION, constants.LONG_FLASH_TIMES);
            else if (response === constants.WRAPPER_RESPONSE_API_ERROR)
                await this.#flashImage(ev.action, 'images/states/api-error', constants.LONG_FLASH_DURATION, constants.LONG_FLASH_TIMES);
            else if (response === constants.WRAPPER_RESPONSE_FATAL_ERROR)
                await this.#flashImage(ev.action, 'images/states/fatal-error', constants.LONG_FLASH_DURATION, constants.LONG_FLASH_TIMES);
            else if (response === constants.WRAPPER_RESPONSE_NO_DEVICE_ERROR)
                await this.#flashImage(ev.action, 'images/states/no-device-error', constants.LONG_FLASH_DURATION, constants.LONG_FLASH_TIMES);
            else if (response === constants.WRAPPER_RESPONSE_BUSY)
                await this.#flashImage(ev.action, 'images/states/busy', constants.SHORT_FLASH_DURATION, constants.SHORT_FLASH_TIMES);
        }
    }
    async onKeyUp(ev) {
        if (this.#busy[ev.action.id] || this.#unpressable[ev.action.id] || this.constructor.ACTIONLESS) {
            let wasReleasedBeforeHoldDelay = false;
            let wasHeld = false;
            if (this.#holding[ev.action.id] && (!this.#holding[ev.action.id].held)) {
                const resolve = this.#holding[ev.action.id].resolve;
                clearTimeout(this.#holding[ev.action.id].timeout);
                delete this.#holding[ev.action.id];
                resolve(true);
                wasReleasedBeforeHoldDelay = true;
            }
            else if (this.#holding[ev.action.id]) {
                wasHeld = true;
                clearTimeout(this.#holding[ev.action.id].timeout);
                delete this.#holding[ev.action.id];
            }
            if ((!this.constructor.MULTI) || wasHeld) {
                if ((!this.#unpressable[ev.action.id]) && (!this.constructor.ACTIONLESS)) {
                    if (wasReleasedBeforeHoldDelay)
                        await this.#invokePress(ev, false, 1);
                    if (this.#pressed[ev.action.id]?.invoked || wasReleasedBeforeHoldDelay)
                        await this.invokeWrapperAction(ev.action.id, _a$1.TYPES.RELEASED);
                }
                if (this.#pressed[ev.action.id]) {
                    clearTimeout(this.#pressed[ev.action.id].timeout);
                    delete this.#pressed[ev.action.id];
                }
                if (wasReleasedBeforeHoldDelay)
                    delete this.#busy[ev.action.id];
                return;
            }
        }
        this.#busy[ev.action.id] = true;
        if (this.#holding[ev.action.id]?.resolve)
            this.#holding[ev.action.id].resolve(true);
        clearTimeout(this.#pressed[ev.action.id]?.timeout);
        clearTimeout(this.#holding[ev.action.id]?.timeout);
        if (this.#pressed[ev.action.id])
            if ((!this.#holding[ev.action.id]) && this.constructor.MULTI && (!this.#pressed[ev.action.id].long))
                if ((!this.#keyUpTracker[ev.action.id]) || this.#keyUpTracker[ev.action.id].presses < 3) {
                    clearTimeout(this.#keyUpTracker[ev.action.id]?.timeout);
                    this.#keyUpTracker[ev.action.id] = {
                        time: Date.now(),
                        presses: (this.#keyUpTracker[ev.action.id]?.presses || 0) + 1,
                        timeout: setTimeout(async () => {
                            this.#busy[ev.action.id] = true;
                            await this.#invokePress(ev, false, this.#keyUpTracker[ev.action.id].presses);
                            await this.invokeWrapperAction(ev.action.id, _a$1.TYPES.RELEASED);
                            delete this.#keyUpTracker[ev.action.id];
                            delete this.#busy[ev.action.id];
                        }, constants.BUTTON_MULTI_PRESS_INTERVAL)
                    };
                }
        if (this.#pressed[ev.action.id]?.invoked)
            await this.invokeWrapperAction(ev.action.id, _a$1.TYPES.RELEASED);
        delete this.#pressed[ev.action.id];
        delete this.#holding[ev.action.id];
        delete this.#busy[ev.action.id];
    }
    async onWillAppear(ev) {
        await super.onWillAppear(ev);
        await this.setImage(ev.action.id, this.#lastImage[ev.action.id] ?? (this.constructor.STATABLE && (!connector.set) ? this.#statelessImage : undefined));
        await this.setTitle(ev.action.id, this.#lastTitle[ev.action.id] ?? '');
        if (this.constructor.STATABLE)
            if (!connector.set)
                await this.onStateLoss(ev.action.id);
            else
                await this.onStateSettled(ev.action.id, true);
    }
    async onWillDisappear(ev) {
        await super.onWillDisappear(ev);
        if (this.#flashing[ev.action.id])
            await this.#flashing[ev.action.id];
        if (this.#holding[ev.action.id]?.resolve)
            this.#holding[ev.action.id].resolve(true);
        clearTimeout(this.#pressed[ev.action.id]?.timeout);
        clearTimeout(this.#holding[ev.action.id]?.timeout);
        clearTimeout(this.#keyUpTracker[ev.action.id]?.timeout);
        delete this.#pressed[ev.action.id];
        delete this.#holding[ev.action.id];
        delete this.#keyUpTracker[ev.action.id];
    }
    async invokeWrapperAction(context, type) {
        return constants.WRAPPER_RESPONSE_NOT_AVAILABLE;
    }
    async setTitle(context, title) {
        this.#lastTitle[context] = title;
        if (!this.#flashing[context])
            await this.actionObjects.get(context)?.setTitle(title).catch((e) => logger.error(`An error occurred while setting the Stream Deck title of "${this.manifestId}": "${e.message || 'No message.'}" @ "${e.stack || 'No stack trace.'}".`));
    }
    async setImage(context, image) {
        this.#lastImage[context] = image;
        if (!this.#flashing[context])
            await this.actionObjects.get(context)?.setImage(image).catch((e) => logger.error(`An error occurred while setting the Stream Deck image of "${this.manifestId}": "${e.message || 'No message.'}" @ "${e.stack || 'No stack trace.'}".`));
    }
    async setState(context, state) {
        await this.actionObjects.get(context)?.setState(state).catch((e) => logger.error(`An error occurred while setting the Stream Deck state of "${this.manifestId}": "${e.message || 'No message.'}" @ "${e.stack || 'No stack trace.'}".`));
    }
    async onStateSettled(context, skipImageReset = false) {
        if (!skipImageReset)
            await this.setImage(context);
    }
    async onStateLoss(context) {
        await this.setImage(context, this.#statelessImage);
    }
    async marqueeTitle(id, data, context, forced = false) {
        if (this.#flashing[context])
            return;
        data = data.filter(item => !!item);
        if (data.length === 0) {
            this.clearMarquee(context);
            this.setTitle(context, '');
            return;
        }
        const isInitial = !this.marquees[context];
        const marqueeData = this.marquees[context] || {
            timeout: null,
            id,
            data,
            entries: {}
        };
        if (!this.marquees[context])
            for (let i = 0; i < data.length; i++) {
                marqueeData.entries[data[i].key] = {
                    original: data[i].value,
                    render: `${data[i].value}${' '.repeat(constants.BUTTON_MARQUEE_SPACING * constants.BUTTON_MARQUEE_SPACING_MULTIPLIER)}`,
                    frame: null,
                    totalFrames: null
                };
            }
        if (this.marquees[context] && this.marquees[context].id !== id)
            return;
        this.marquees[context] = marqueeData;
        let finalText = '';
        for (let i = 0; i < data.length; i++) {
            if (marqueeData.entries[data[i].key].frame === null)
                marqueeData.entries[data[i].key].frame = (marqueeData.entries[data[i].key].original.length / 2) + constants.BUTTON_MARQUEE_SPACING;
            if (marqueeData.entries[data[i].key].totalFrames === null)
                marqueeData.entries[data[i].key].totalFrames = marqueeData.entries[data[i].key].render.length;
            finalText += `${this.getTextSpacingWidth(marqueeData.entries[data[i].key].original) > constants.BUTTON_MARQUEE_SPACING ? `${marqueeData.entries[data[i].key].render.slice(marqueeData.entries[data[i].key].frame)}${marqueeData.entries[data[i].key].render.slice(0, marqueeData.entries[data[i].key].frame)}` : marqueeData.entries[data[i].key].original}\n`;
        }
        this.setTitle(context, finalText.slice(0, -1));
        if ((!this.marquees[context]) || this.marquees[context].id !== id)
            return;
        for (let i = 0; i < data.length; i++) {
            marqueeData.entries[data[i].key].frame++;
            if (marqueeData.entries[data[i].key].frame >= marqueeData.entries[data[i].key].totalFrames)
                marqueeData.entries[data[i].key].frame = 0;
        }
        if (!forced)
            marqueeData.timeout = setTimeout(() => this.marqueeTitle(id, marqueeData.data, context), isInitial ? constants.BUTTON_MARQUEE_INTERVAL_INITIAL : constants.BUTTON_MARQUEE_INTERVAL);
    }
    updateMarqueeEntry(context, key, value) {
        if (this.marquees[context] && this.marquees[context].entries[key]) {
            this.marquees[context].entries[key].original = value;
            this.marquees[context].entries[key].render = `${value}${' '.repeat(constants.BUTTON_MARQUEE_SPACING * constants.BUTTON_MARQUEE_SPACING_MULTIPLIER)}`;
            this.marquees[context].entries[key].totalFrames = this.marquees[context].entries[key].render.length;
        }
    }
    resumeMarquee(context, forced = false) {
        if (this.marquees[context]) {
            clearTimeout(this.marquees[context].timeout);
            if (forced)
                this.marqueeTitle(this.marquees[context].id, this.marquees[context].data, context, true);
            this.marquees[context].timeout = setTimeout(() => this.marqueeTitle(this.marquees[context].id, this.marquees[context].data, context), constants.BUTTON_MARQUEE_INTERVAL);
        }
    }
    pauseMarquee(context) {
        if (this.marquees[context]) {
            clearTimeout(this.marquees[context].timeout);
            this.marquees[context].timeout = null;
        }
    }
    clearMarquee(context) {
        if (this.marquees[context]) {
            clearTimeout(this.marquees[context].timeout);
            delete this.marquees[context];
        }
    }
    getTextSpacingWidth(text) {
        let totalWidth = 0;
        for (const char of text)
            totalWidth += constants.CHARACTER_WIDTH_MAP[char] || 1;
        return totalWidth;
    }
    setStatelessImage(image) {
        this.#statelessImage = image;
    }
    setUnpressable(context, busy) {
        if (!busy)
            delete this.#unpressable[context];
        else
            this.#unpressable[context] = busy;
    }
}
_a$1 = Button;

let SetupButton = (() => {
    let _classDecorators = [action({ UUID: 'com.ntanis.essentials-for-spotify.setup-button' })];
    let _classDescriptor;
    let _classExtraInitializers = [];
    let _classThis;
    let _classSuper = Button;
    (class extends _classSuper {
        static { _classThis = this; }
        static {
            const _metadata = typeof Symbol === "function" && Symbol.metadata ? Object.create(_classSuper[Symbol.metadata] ?? null) : void 0;
            __esDecorate(null, _classDescriptor = { value: _classThis }, _classDecorators, { kind: "class", name: _classThis.name, metadata: _metadata }, null, _classExtraInitializers);
            _classThis = _classDescriptor.value;
            if (_metadata) Object.defineProperty(_classThis, Symbol.metadata, { enumerable: true, configurable: true, writable: true, value: _metadata });
        }
        static SETUPLESS = true;
        static STATABLE = true;
        FAKE = false;
        constructor() {
            super();
            connector.on('setupStateChanged', this.#onSetupStateChanged.bind(this));
        }
        #onSetupStateChanged(state) {
            const promises = [];
            for (const context of this.contexts)
                promises.push(this.setState(context, state ? 1 : 0));
            return Promise.allSettled(promises);
        }
        async invokeWrapperAction(context, type) {
            if (type === Button.TYPES.RELEASED)
                return;
            if (this.FAKE)
                if (connector.set)
                    connector.fakeOff();
                else
                    connector.fakeOn();
            else {
                if (connector.set)
                    connector.invalidateSetup();
                exec(`${process.platform == 'darwin' ? 'open' : (process.platform == 'win32' ? 'start' : 'xdg-open')} http://127.0.0.1:${connector.port || constants.CONNECTOR_DEFAULT_PORT}/?lang=${streamDeck.info.application.language}`, (error, stdout, stderr) => {
                    if (error)
                        logger.error(`An error occurred while opening browser: "${error.message || 'No message.'}" @ "${error.stack || 'No stack trace.'}".`);
                });
            }
            return constants.WRAPPER_RESPONSE_SUCCESS_INDICATIVE;
        }
        async onStateSettled(context) {
            await super.onStateSettled(context);
            await this.#onSetupStateChanged(true);
        }
        async onStateLoss(context) {
            await super.onStateLoss(context);
            await this.#onSetupStateChanged(false);
        }
        static {
            __runInitializers(_classThis, _classExtraInitializers);
        }
    });
    return _classThis;
})();

let PlayPauseButton = (() => {
    let _classDecorators = [action({ UUID: 'com.ntanis.essentials-for-spotify.play-pause-button' })];
    let _classDescriptor;
    let _classExtraInitializers = [];
    let _classThis;
    let _classSuper = Button;
    (class extends _classSuper {
        static { _classThis = this; }
        static {
            const _metadata = typeof Symbol === "function" && Symbol.metadata ? Object.create(_classSuper[Symbol.metadata] ?? null) : void 0;
            __esDecorate(null, _classDescriptor = { value: _classThis }, _classDecorators, { kind: "class", name: _classThis.name, metadata: _metadata }, null, _classExtraInitializers);
            _classThis = _classDescriptor.value;
            if (_metadata) Object.defineProperty(_classThis, Symbol.metadata, { enumerable: true, configurable: true, writable: true, value: _metadata });
            __runInitializers(_classThis, _classExtraInitializers);
        }
        constructor() {
            super();
            wrapper.on('playbackStateChanged', this.#onPlaybackStateChanged.bind(this));
        }
        async #onPlaybackStateChanged(state, contexts = this.contexts) {
            const promises = [];
            for (const context of contexts)
                if (this.settings[context].action === 'play_pause')
                    promises.push(this.setState(context, state ? 1 : 0));
            await Promise.allSettled(promises);
        }
        async invokeWrapperAction(context, type) {
            if (type === Button.TYPES.RELEASED)
                return;
            if (wrapper.playing && this.settings[context].action !== 'play')
                return wrapper.pausePlayback();
            else if ((!wrapper.playing) && this.settings[context].action !== 'pause')
                return wrapper.resumePlayback();
        }
        async onWillAppear(ev) {
            await super.onWillAppear(ev);
            await this.#onPlaybackStateChanged(wrapper.playing, [ev.action.id]);
        }
        async onSettingsUpdated(context, oldSettings) {
            await super.onSettingsUpdated(context, oldSettings);
            if (!this.settings[context].action)
                await this.setSettings(context, {
                    action: 'play_pause'
                });
            switch (this.settings[context].action) {
                case 'play_pause':
                    await this.#onPlaybackStateChanged(wrapper.playing, [context]);
                    break;
                case 'play':
                    await this.setState(context, 0);
                    break;
                case 'pause':
                    await this.setState(context, 1);
                    break;
            }
        }
    });
    return _classThis;
})();

let PreviousSongButton = (() => {
    let _classDecorators = [action({ UUID: 'com.ntanis.essentials-for-spotify.previous-song-button' })];
    let _classDescriptor;
    let _classExtraInitializers = [];
    let _classThis;
    let _classSuper = Button;
    (class extends _classSuper {
        static { _classThis = this; }
        static {
            const _metadata = typeof Symbol === "function" && Symbol.metadata ? Object.create(_classSuper[Symbol.metadata] ?? null) : void 0;
            __esDecorate(null, _classDescriptor = { value: _classThis }, _classDecorators, { kind: "class", name: _classThis.name, metadata: _metadata }, null, _classExtraInitializers);
            _classThis = _classDescriptor.value;
            if (_metadata) Object.defineProperty(_classThis, Symbol.metadata, { enumerable: true, configurable: true, writable: true, value: _metadata });
        }
        static HOLDABLE = true;
        async invokeWrapperAction(context, type) {
            if (type === Button.TYPES.RELEASED) {
                await this.setImage(context);
                return;
            }
            if (type === Button.TYPES.SINGLE_PRESS)
                return wrapper.previousSong();
            else if (type === Button.TYPES.HOLDING) {
                await this.setImage(context, 'images/states/backward-seek');
                return wrapper.backwardSeek(this.settings[context].step);
            }
        }
        async onSettingsUpdated(context, oldSettings) {
            await super.onSettingsUpdated(context, oldSettings);
            if (!this.settings[context].step)
                await this.setSettings(context, {
                    step: constants.DEFAULT_SEEK_STEP_SIZE
                });
        }
        static {
            __runInitializers(_classThis, _classExtraInitializers);
        }
    });
    return _classThis;
})();

let NextSongButton = (() => {
    let _classDecorators = [action({ UUID: 'com.ntanis.essentials-for-spotify.next-song-button' })];
    let _classDescriptor;
    let _classExtraInitializers = [];
    let _classThis;
    let _classSuper = Button;
    (class extends _classSuper {
        static { _classThis = this; }
        static {
            const _metadata = typeof Symbol === "function" && Symbol.metadata ? Object.create(_classSuper[Symbol.metadata] ?? null) : void 0;
            __esDecorate(null, _classDescriptor = { value: _classThis }, _classDecorators, { kind: "class", name: _classThis.name, metadata: _metadata }, null, _classExtraInitializers);
            _classThis = _classDescriptor.value;
            if (_metadata) Object.defineProperty(_classThis, Symbol.metadata, { enumerable: true, configurable: true, writable: true, value: _metadata });
        }
        static HOLDABLE = true;
        async invokeWrapperAction(context, type) {
            if (type === Button.TYPES.RELEASED) {
                await this.setImage(context);
                return;
            }
            if (type === Button.TYPES.SINGLE_PRESS)
                return wrapper.nextSong();
            else if (type === Button.TYPES.HOLDING) {
                await this.setImage(context, 'images/states/forward-seek');
                return wrapper.forwardSeek(this.settings[context].step);
            }
        }
        async onSettingsUpdated(context, oldSettings) {
            await super.onSettingsUpdated(context, oldSettings);
            if (!this.settings[context].step)
                await this.setSettings(context, {
                    step: constants.DEFAULT_SEEK_STEP_SIZE
                });
        }
        static {
            __runInitializers(_classThis, _classExtraInitializers);
        }
    });
    return _classThis;
})();

let BackwardSeekButton = (() => {
    let _classDecorators = [action({ UUID: 'com.ntanis.essentials-for-spotify.backward-seek-button' })];
    let _classDescriptor;
    let _classExtraInitializers = [];
    let _classThis;
    let _classSuper = Button;
    (class extends _classSuper {
        static { _classThis = this; }
        static {
            const _metadata = typeof Symbol === "function" && Symbol.metadata ? Object.create(_classSuper[Symbol.metadata] ?? null) : void 0;
            __esDecorate(null, _classDescriptor = { value: _classThis }, _classDecorators, { kind: "class", name: _classThis.name, metadata: _metadata }, null, _classExtraInitializers);
            _classThis = _classDescriptor.value;
            if (_metadata) Object.defineProperty(_classThis, Symbol.metadata, { enumerable: true, configurable: true, writable: true, value: _metadata });
        }
        static HOLDABLE = true;
        async invokeWrapperAction(context, type) {
            if (type === Button.TYPES.RELEASED)
                return;
            return wrapper.backwardSeek(this.settings[context].step);
        }
        async onSettingsUpdated(context, oldSettings) {
            await super.onSettingsUpdated(context, oldSettings);
            if (!this.settings[context].step)
                await this.setSettings(context, {
                    step: constants.DEFAULT_SEEK_STEP_SIZE
                });
        }
        static {
            __runInitializers(_classThis, _classExtraInitializers);
        }
    });
    return _classThis;
})();

let ForwardSeekButton = (() => {
    let _classDecorators = [action({ UUID: 'com.ntanis.essentials-for-spotify.forward-seek-button' })];
    let _classDescriptor;
    let _classExtraInitializers = [];
    let _classThis;
    let _classSuper = Button;
    (class extends _classSuper {
        static { _classThis = this; }
        static {
            const _metadata = typeof Symbol === "function" && Symbol.metadata ? Object.create(_classSuper[Symbol.metadata] ?? null) : void 0;
            __esDecorate(null, _classDescriptor = { value: _classThis }, _classDecorators, { kind: "class", name: _classThis.name, metadata: _metadata }, null, _classExtraInitializers);
            _classThis = _classDescriptor.value;
            if (_metadata) Object.defineProperty(_classThis, Symbol.metadata, { enumerable: true, configurable: true, writable: true, value: _metadata });
        }
        static HOLDABLE = true;
        async invokeWrapperAction(context, type) {
            if (type === Button.TYPES.RELEASED)
                return;
            return wrapper.forwardSeek(this.settings[context].step);
        }
        async onSettingsUpdated(context, oldSettings) {
            await super.onSettingsUpdated(context, oldSettings);
            if (!this.settings[context].step)
                await this.setSettings(context, {
                    step: constants.DEFAULT_SEEK_STEP_SIZE
                });
        }
        static {
            __runInitializers(_classThis, _classExtraInitializers);
        }
    });
    return _classThis;
})();

let ShuffleButton = (() => {
    let _classDecorators = [action({ UUID: 'com.ntanis.essentials-for-spotify.shuffle-button' })];
    let _classDescriptor;
    let _classExtraInitializers = [];
    let _classThis;
    let _classSuper = Button;
    (class extends _classSuper {
        static { _classThis = this; }
        static {
            const _metadata = typeof Symbol === "function" && Symbol.metadata ? Object.create(_classSuper[Symbol.metadata] ?? null) : void 0;
            __esDecorate(null, _classDescriptor = { value: _classThis }, _classDecorators, { kind: "class", name: _classThis.name, metadata: _metadata }, null, _classExtraInitializers);
            _classThis = _classDescriptor.value;
            if (_metadata) Object.defineProperty(_classThis, Symbol.metadata, { enumerable: true, configurable: true, writable: true, value: _metadata });
        }
        static STATABLE = true;
        constructor() {
            super();
            this.setStatelessImage('images/states/shuffle-unknown');
            wrapper.on('shuffleStateChanged', this.#onShuffleStateChanged.bind(this));
        }
        async #onShuffleStateChanged(state, contexts = this.contexts) {
            const promises = [];
            for (const context of contexts)
                promises.push(this.setState(context, state ? 1 : 0));
            return Promise.allSettled(promises);
        }
        async invokeWrapperAction(context, type) {
            if (type === Button.TYPES.RELEASED)
                return;
            return wrapper.toggleShuffle();
        }
        async onStateSettled(context) {
            await super.onStateSettled(context);
            await this.#onShuffleStateChanged(wrapper.shuffleState, [context]);
        }
        static {
            __runInitializers(_classThis, _classExtraInitializers);
        }
    });
    return _classThis;
})();

let LoopContextButton = (() => {
    let _classDecorators = [action({ UUID: 'com.ntanis.essentials-for-spotify.loop-context-button' })];
    let _classDescriptor;
    let _classExtraInitializers = [];
    let _classThis;
    let _classSuper = Button;
    (class extends _classSuper {
        static { _classThis = this; }
        static {
            const _metadata = typeof Symbol === "function" && Symbol.metadata ? Object.create(_classSuper[Symbol.metadata] ?? null) : void 0;
            __esDecorate(null, _classDescriptor = { value: _classThis }, _classDecorators, { kind: "class", name: _classThis.name, metadata: _metadata }, null, _classExtraInitializers);
            _classThis = _classDescriptor.value;
            if (_metadata) Object.defineProperty(_classThis, Symbol.metadata, { enumerable: true, configurable: true, writable: true, value: _metadata });
        }
        static STATABLE = true;
        constructor() {
            super();
            this.setStatelessImage('images/states/loop-unknown');
            wrapper.on('repeatStateChanged', this.#onRepeatStateChanged.bind(this));
        }
        async #onRepeatStateChanged(state, contexts = this.contexts) {
            const promises = [];
            for (const context of contexts)
                promises.push(this.setState(context, state === 'context' ? 1 : 0));
            await Promise.allSettled(promises);
        }
        async invokeWrapperAction(context, type) {
            if (type === Button.TYPES.RELEASED)
                return;
            return wrapper.toggleContextRepeat();
        }
        async onStateSettled(context) {
            await super.onStateSettled(context);
            await this.#onRepeatStateChanged(wrapper.repeatState, [context]);
        }
        static {
            __runInitializers(_classThis, _classExtraInitializers);
        }
    });
    return _classThis;
})();

let LoopSongButton = (() => {
    let _classDecorators = [action({ UUID: 'com.ntanis.essentials-for-spotify.loop-song-button' })];
    let _classDescriptor;
    let _classExtraInitializers = [];
    let _classThis;
    let _classSuper = Button;
    (class extends _classSuper {
        static { _classThis = this; }
        static {
            const _metadata = typeof Symbol === "function" && Symbol.metadata ? Object.create(_classSuper[Symbol.metadata] ?? null) : void 0;
            __esDecorate(null, _classDescriptor = { value: _classThis }, _classDecorators, { kind: "class", name: _classThis.name, metadata: _metadata }, null, _classExtraInitializers);
            _classThis = _classDescriptor.value;
            if (_metadata) Object.defineProperty(_classThis, Symbol.metadata, { enumerable: true, configurable: true, writable: true, value: _metadata });
        }
        static STATABLE = true;
        constructor() {
            super();
            this.setStatelessImage('images/states/loop-one-unknown');
            wrapper.on('repeatStateChanged', this.#onRepeatStateChanged.bind(this));
        }
        async #onRepeatStateChanged(state, contexts = this.contexts) {
            const promises = [];
            for (const context of contexts)
                promises.push(this.setState(context, state === 'track' ? 1 : 0));
            await Promise.allSettled(promises);
        }
        async invokeWrapperAction(context, type) {
            if (type === Button.TYPES.RELEASED)
                return;
            return wrapper.toggleTrackRepeat();
        }
        async onStateSettled(context) {
            await super.onStateSettled(context);
            await this.#onRepeatStateChanged(wrapper.repeatState, [context]);
        }
        static {
            __runInitializers(_classThis, _classExtraInitializers);
        }
    });
    return _classThis;
})();

let ModeStackButton = (() => {
    let _classDecorators = [action({ UUID: 'com.ntanis.essentials-for-spotify.mode-stack-button' })];
    let _classDescriptor;
    let _classExtraInitializers = [];
    let _classThis;
    let _classSuper = Button;
    (class extends _classSuper {
        static { _classThis = this; }
        static {
            const _metadata = typeof Symbol === "function" && Symbol.metadata ? Object.create(_classSuper[Symbol.metadata] ?? null) : void 0;
            __esDecorate(null, _classDescriptor = { value: _classThis }, _classDecorators, { kind: "class", name: _classThis.name, metadata: _metadata }, null, _classExtraInitializers);
            _classThis = _classDescriptor.value;
            if (_metadata) Object.defineProperty(_classThis, Symbol.metadata, { enumerable: true, configurable: true, writable: true, value: _metadata });
        }
        static STATABLE = true;
        static MULTI = true;
        constructor() {
            super();
            this.setStatelessImage('images/states/mode-stack-unknown');
            wrapper.on('shuffleStateChanged', (state) => this.#onStateChanged());
            wrapper.on('repeatStateChanged', (state) => this.#onStateChanged());
        }
        async #onStateChanged(contexts = this.contexts) {
            const promises = [];
            for (const context of contexts)
                promises.push(this.setImage(context, `images/states/mode-stack-${wrapper.shuffleState ? '1' : '0'}-${wrapper.repeatState === 'track' ? '1' : '0'}-${wrapper.repeatState === 'context' ? '1' : '0'}`));
            return Promise.allSettled(promises);
        }
        async #invokePress(context, action) {
            switch (action) {
                case 'shuffle':
                    return wrapper.toggleShuffle();
                case 'loop_song':
                    return wrapper.toggleTrackRepeat();
                case 'loop_context':
                    return wrapper.toggleContextRepeat();
                case 'reset_all': {
                    const promises = [];
                    if (wrapper.shuffleState)
                        promises.push(wrapper.turnOffShuffle());
                    if (wrapper.repeatState !== 'off')
                        promises.push(wrapper.turnOffRepeat());
                    return Promise.allSettled(promises);
                }
            }
        }
        async invokeWrapperAction(context, type) {
            if (type === Button.TYPES.RELEASED)
                return;
            if (type === Button.TYPES.SINGLE_PRESS)
                return this.#invokePress(context, this.settings[context].single_press);
            else if (type === Button.TYPES.DOUBLE_PRESS)
                return this.#invokePress(context, this.settings[context].double_press);
            else if (type === Button.TYPES.TRIPLE_PRESS)
                return this.#invokePress(context, this.settings[context].triple_press);
            else if (type === Button.TYPES.LONG_PRESS)
                return this.#invokePress(context, this.settings[context].long_press);
            else
                return constants.WRAPPER_RESPONSE_NOT_AVAILABLE;
        }
        async onSettingsUpdated(context, oldSettings) {
            await super.onSettingsUpdated(context, oldSettings);
            if (!this.settings[context].single_press)
                await this.setSettings(context, {
                    single_press: 'shuffle'
                });
            if (!this.settings[context].double_press)
                await this.setSettings(context, {
                    double_press: 'loop_song'
                });
            if (!this.settings[context].triple_press)
                await this.setSettings(context, {
                    triple_press: 'loop_context'
                });
            if (!this.settings[context].long_press)
                await this.setSettings(context, {
                    long_press: 'reset_all'
                });
        }
        async onStateSettled(context) {
            await super.onStateSettled(context);
            await this.#onStateChanged([context]);
        }
        static {
            __runInitializers(_classThis, _classExtraInitializers);
        }
    });
    return _classThis;
})();

let LikeUnlikeButton = (() => {
    let _classDecorators = [action({ UUID: 'com.ntanis.essentials-for-spotify.like-unlike-button' })];
    let _classDescriptor;
    let _classExtraInitializers = [];
    let _classThis;
    let _classSuper = Button;
    (class extends _classSuper {
        static { _classThis = this; }
        static {
            const _metadata = typeof Symbol === "function" && Symbol.metadata ? Object.create(_classSuper[Symbol.metadata] ?? null) : void 0;
            __esDecorate(null, _classDescriptor = { value: _classThis }, _classDecorators, { kind: "class", name: _classThis.name, metadata: _metadata }, null, _classExtraInitializers);
            _classThis = _classDescriptor.value;
            if (_metadata) Object.defineProperty(_classThis, Symbol.metadata, { enumerable: true, configurable: true, writable: true, value: _metadata });
        }
        static STATABLE = true;
        constructor() {
            super();
            this.setStatelessImage('images/states/like-unknown');
            wrapper.on('songChanged', (...args) => this.#onLikedStateChanged(wrapper.song?.liked, wrapper.pendingSongChange));
            wrapper.on('songLikedStateChanged', this.#onLikedStateChanged.bind(this));
        }
        async #onLikedStateChanged(liked, pending = false, contexts = this.contexts) {
            const promises = [];
            for (const context of contexts)
                promises.push(new Promise(async (resolve) => {
                    this.setUnpressable(context, true);
                    await this.setImage(context, pending ? 'images/states/pending' : undefined);
                    if (!pending) {
                        if (wrapper.song) {
                            await this.setImage(context);
                            await this.setState(context, liked ? 1 : 0);
                        }
                        else
                            await this.setImage(context, 'images/states/like-unknown');
                        this.setUnpressable(context, false);
                    }
                    resolve(true);
                }));
            await Promise.allSettled(promises);
        }
        async invokeWrapperAction(context, type) {
            if (type === Button.TYPES.RELEASED)
                return;
            return wrapper.toggleCurrentSongLike();
        }
        async onStateSettled(context) {
            await super.onStateSettled(context);
            await this.#onLikedStateChanged(wrapper.song?.liked, wrapper.pendingSongChange, [context]);
        }
        static {
            __runInitializers(_classThis, _classExtraInitializers);
        }
    });
    return _classThis;
})();

let SongExplicitButton = (() => {
    let _classDecorators = [action({ UUID: 'com.ntanis.essentials-for-spotify.song-explicit-button' })];
    let _classDescriptor;
    let _classExtraInitializers = [];
    let _classThis;
    let _classSuper = Button;
    (class extends _classSuper {
        static { _classThis = this; }
        static {
            const _metadata = typeof Symbol === "function" && Symbol.metadata ? Object.create(_classSuper[Symbol.metadata] ?? null) : void 0;
            __esDecorate(null, _classDescriptor = { value: _classThis }, _classDecorators, { kind: "class", name: _classThis.name, metadata: _metadata }, null, _classExtraInitializers);
            _classThis = _classDescriptor.value;
            if (_metadata) Object.defineProperty(_classThis, Symbol.metadata, { enumerable: true, configurable: true, writable: true, value: _metadata });
        }
        static STATABLE = true;
        static ACTIONLESS = true;
        constructor() {
            super();
            this.setStatelessImage('images/states/song-explicit-unknown');
            wrapper.on('songChanged', this.#onSongChanged.bind(this));
        }
        async #onSongChanged(song, pending = false, contexts = this.contexts) {
            const promises = [];
            for (const context of contexts)
                promises.push(new Promise(async (resolve) => {
                    this.setUnpressable(context, true);
                    await this.setImage(context, pending ? 'images/states/pending' : undefined);
                    if (!pending) {
                        if (song) {
                            await this.setImage(context);
                            await this.setState(context, song?.item.explicit ? 1 : 0);
                        }
                        else
                            await this.setImage(context, 'images/states/song-explicit-unknown');
                        this.setUnpressable(context, false);
                    }
                    resolve(true);
                }));
            await Promise.allSettled(promises);
        }
        async onStateSettled(context) {
            await super.onStateSettled(context);
            await this.#onSongChanged(wrapper.song, false, [context]);
        }
        static {
            __runInitializers(_classThis, _classExtraInitializers);
        }
    });
    return _classThis;
})();

let VolumeUpButton = (() => {
    let _classDecorators = [action({ UUID: 'com.ntanis.essentials-for-spotify.volume-up-button' })];
    let _classDescriptor;
    let _classExtraInitializers = [];
    let _classThis;
    let _classSuper = Button;
    (class extends _classSuper {
        static { _classThis = this; }
        static {
            const _metadata = typeof Symbol === "function" && Symbol.metadata ? Object.create(_classSuper[Symbol.metadata] ?? null) : void 0;
            __esDecorate(null, _classDescriptor = { value: _classThis }, _classDecorators, { kind: "class", name: _classThis.name, metadata: _metadata }, null, _classExtraInitializers);
            _classThis = _classDescriptor.value;
            if (_metadata) Object.defineProperty(_classThis, Symbol.metadata, { enumerable: true, configurable: true, writable: true, value: _metadata });
        }
        static HOLDABLE = true;
        async invokeWrapperAction(context, type) {
            if (type === Button.TYPES.RELEASED)
                return;
            if (wrapper.volumePercent === null)
                return constants.WRAPPER_RESPONSE_NOT_AVAILABLE;
            return wrapper.volumeUp(this.settings[context].step);
        }
        async onSettingsUpdated(context, oldSettings) {
            await super.onSettingsUpdated(context, oldSettings);
            if (!this.settings[context].step)
                await this.setSettings(context, {
                    step: constants.DEFAULT_VOLUME_STEP
                });
        }
        static {
            __runInitializers(_classThis, _classExtraInitializers);
        }
    });
    return _classThis;
})();

let VolumeDownButton = (() => {
    let _classDecorators = [action({ UUID: 'com.ntanis.essentials-for-spotify.volume-down-button' })];
    let _classDescriptor;
    let _classExtraInitializers = [];
    let _classThis;
    let _classSuper = Button;
    (class extends _classSuper {
        static { _classThis = this; }
        static {
            const _metadata = typeof Symbol === "function" && Symbol.metadata ? Object.create(_classSuper[Symbol.metadata] ?? null) : void 0;
            __esDecorate(null, _classDescriptor = { value: _classThis }, _classDecorators, { kind: "class", name: _classThis.name, metadata: _metadata }, null, _classExtraInitializers);
            _classThis = _classDescriptor.value;
            if (_metadata) Object.defineProperty(_classThis, Symbol.metadata, { enumerable: true, configurable: true, writable: true, value: _metadata });
        }
        static HOLDABLE = true;
        async invokeWrapperAction(context, type) {
            if (type === Button.TYPES.RELEASED)
                return;
            if (wrapper.volumePercent === null)
                return constants.WRAPPER_RESPONSE_NOT_AVAILABLE;
            return wrapper.volumeDown(this.settings[context].step);
        }
        async onSettingsUpdated(context, oldSettings) {
            await super.onSettingsUpdated(context, oldSettings);
            if (!this.settings[context].step)
                await this.setSettings(context, {
                    step: constants.DEFAULT_VOLUME_STEP
                });
        }
        static {
            __runInitializers(_classThis, _classExtraInitializers);
        }
    });
    return _classThis;
})();

let VolumeMuteUnmuteButton = (() => {
    let _classDecorators = [action({ UUID: 'com.ntanis.essentials-for-spotify.volume-mute-unmute-button' })];
    let _classDescriptor;
    let _classExtraInitializers = [];
    let _classThis;
    let _classSuper = Button;
    (class extends _classSuper {
        static { _classThis = this; }
        static {
            const _metadata = typeof Symbol === "function" && Symbol.metadata ? Object.create(_classSuper[Symbol.metadata] ?? null) : void 0;
            __esDecorate(null, _classDescriptor = { value: _classThis }, _classDecorators, { kind: "class", name: _classThis.name, metadata: _metadata }, null, _classExtraInitializers);
            _classThis = _classDescriptor.value;
            if (_metadata) Object.defineProperty(_classThis, Symbol.metadata, { enumerable: true, configurable: true, writable: true, value: _metadata });
        }
        static STATABLE = true;
        constructor() {
            super();
            this.setStatelessImage('images/states/volume-mute-unknown');
            wrapper.on('mutedStateChanged', this.#onMutedStateChanged.bind(this));
        }
        async #onMutedStateChanged(state, contexts = this.contexts) {
            const promises = [];
            for (const context of contexts)
                promises.push(new Promise(async (resolve) => {
                    if (wrapper.volumePercent === null)
                        await this.setImage(context, 'images/states/volume-mute-unknown');
                    else {
                        await this.setImage(context);
                        await this.setState(context, state ? 1 : 0);
                    }
                    resolve(true);
                }));
            await Promise.allSettled(promises);
        }
        async invokeWrapperAction(context, type) {
            if (type === Button.TYPES.RELEASED)
                return;
            return wrapper.toggleVolumeMute();
        }
        async onStateSettled(context) {
            await super.onStateSettled(context);
            await this.#onMutedStateChanged(wrapper.muted, [context]);
        }
        static {
            __runInitializers(_classThis, _classExtraInitializers);
        }
    });
    return _classThis;
})();

let VolumeStackButton = (() => {
    let _classDecorators = [action({ UUID: 'com.ntanis.essentials-for-spotify.volume-stack-button' })];
    let _classDescriptor;
    let _classExtraInitializers = [];
    let _classThis;
    let _classSuper = Button;
    (class extends _classSuper {
        static { _classThis = this; }
        static {
            const _metadata = typeof Symbol === "function" && Symbol.metadata ? Object.create(_classSuper[Symbol.metadata] ?? null) : void 0;
            __esDecorate(null, _classDescriptor = { value: _classThis }, _classDecorators, { kind: "class", name: _classThis.name, metadata: _metadata }, null, _classExtraInitializers);
            _classThis = _classDescriptor.value;
            if (_metadata) Object.defineProperty(_classThis, Symbol.metadata, { enumerable: true, configurable: true, writable: true, value: _metadata });
        }
        static MULTI = true;
        static STATABLE = true;
        constructor() {
            super();
            this.setStatelessImage('images/states/volume-stack-unknown');
            wrapper.on('mutedStateChanged', (state) => this.#onStateChanged());
            wrapper.on('volumePercentChanged', (percent) => this.#onStateChanged());
        }
        #roundToNext(percent) {
            return Math.max(10, Math.min(100, Math.floor(percent / 10) * 10));
        }
        async #onStateChanged(contexts = this.contexts) {
            const promises = [];
            for (const context of contexts)
                if (((!wrapper.muted) && wrapper.volumePercent === null) || (wrapper.muted && wrapper.mutedVolumePercent === null && wrapper.volumePercent === null))
                    promises.push(this.setImage(context));
                else
                    promises.push(this.setImage(context, `images/states/volume-stack-${this.#roundToNext(wrapper.muted ? (wrapper.mutedVolumePercent ?? wrapper.volumePercent) : wrapper.volumePercent)}${wrapper.muted ? '-muted' : ''}`));
            return Promise.allSettled(promises);
        }
        async #invokePress(context, action) {
            switch (action) {
                case 'volume_up':
                    return wrapper.volumeUp(this.settings[context].step);
                case 'volume_down':
                    return wrapper.volumeDown(this.settings[context].step);
                case 'volume_mute_unmute':
                    return wrapper.toggleVolumeMute();
            }
        }
        async invokeWrapperAction(context, type) {
            if (type === Button.TYPES.RELEASED)
                return;
            if (type === Button.TYPES.SINGLE_PRESS)
                return this.#invokePress(context, this.settings[context].single_press);
            else if (type === Button.TYPES.DOUBLE_PRESS)
                return this.#invokePress(context, this.settings[context].double_press);
            else if (type === Button.TYPES.TRIPLE_PRESS)
                return constants.WRAPPER_RESPONSE_SUCCESS;
            else if (type === Button.TYPES.LONG_PRESS)
                return this.#invokePress(context, this.settings[context].long_press);
            else
                return constants.WRAPPER_RESPONSE_NOT_AVAILABLE;
        }
        async onSettingsUpdated(context, oldSettings) {
            await super.onSettingsUpdated(context, oldSettings);
            if (!this.settings[context].step)
                await this.setSettings(context, {
                    step: constants.DEFAULT_VOLUME_STACK_STEP
                });
            if (!this.settings[context].single_press)
                await this.setSettings(context, {
                    single_press: 'volume_up'
                });
            if (!this.settings[context].double_press)
                await this.setSettings(context, {
                    double_press: 'volume_down'
                });
            if (!this.settings[context].long_press)
                await this.setSettings(context, {
                    long_press: 'volume_mute_unmute'
                });
        }
        async onStateSettled(context) {
            await super.onStateSettled(context, true);
            await this.#onStateChanged([context]);
        }
        static {
            __runInitializers(_classThis, _classExtraInitializers);
        }
    });
    return _classThis;
})();

let PlayContextButton = (() => {
    let _classDecorators = [action({ UUID: 'com.ntanis.essentials-for-spotify.play-context-button' })];
    let _classDescriptor;
    let _classExtraInitializers = [];
    let _classThis;
    let _classSuper = Button;
    (class extends _classSuper {
        static { _classThis = this; }
        static {
            const _metadata = typeof Symbol === "function" && Symbol.metadata ? Object.create(_classSuper[Symbol.metadata] ?? null) : void 0;
            __esDecorate(null, _classDescriptor = { value: _classThis }, _classDecorators, { kind: "class", name: _classThis.name, metadata: _metadata }, null, _classExtraInitializers);
            _classThis = _classDescriptor.value;
            if (_metadata) Object.defineProperty(_classThis, Symbol.metadata, { enumerable: true, configurable: true, writable: true, value: _metadata });
        }
        static STATABLE = true;
        #cachedPlayContexts = {};
        #forcedActiveContexts = {};
        constructor() {
            super();
            this.setStatelessImage('images/states/play-context-unknown');
            wrapper.on('playbackContextChanged', () => {
                for (const context of Object.keys(this.#forcedActiveContexts))
                    if (this.#cachedPlayContexts[context]?.type === 'track' ? wrapper.song?.item?.uri === this.#cachedPlayContexts[context]?.uri : wrapper.playbackContext?.uri === this.#cachedPlayContexts[context]?.uri)
                        delete this.#forcedActiveContexts[context];
                for (const context of this.contexts)
                    this.#updatePlayContext(context, this.settings[context]);
            });
            wrapper.on('songChanged', () => {
                for (const context of Object.keys(this.#forcedActiveContexts))
                    if (this.#cachedPlayContexts[context]?.type === 'track' && wrapper.song?.item?.uri === this.#cachedPlayContexts[context]?.uri)
                        delete this.#forcedActiveContexts[context];
                for (const context of this.contexts)
                    if (this.#cachedPlayContexts[context]?.type === 'track')
                        this.#updatePlayContext(context, this.settings[context]);
            });
        }
        async #updatePlayContext(context, oldSettings = undefined) {
            this.setUnpressable(context, true);
            const badUrl = !/^https?:\/\/open\.spotify\.com\/(?:intl-[a-z]{2}\/)?(?:(?:album|artist|track|playlist)\/[A-Za-z0-9]{22}|collection\/tracks)(?:\/)?(?:\?.*)?$/.test(this.settings[context].spotify_url);
            if ((!oldSettings) || badUrl || this.settings[context].spotify_url !== this.#cachedPlayContexts[context]?.url || (!(oldSettings.show || []).every((entry) => entry === 'active_border' || entry === 'inactive_border' || (this.settings[context].show || []).includes(entry))) || (!(this.settings[context].show || []).every((entry) => entry === 'active_border' || entry === 'inactive_border' || (oldSettings.show || []).includes(entry))))
                this.clearMarquee(context);
            if (this.settings[context].spotify_url !== this.#cachedPlayContexts[context]?.url || badUrl) {
                if (!badUrl)
                    await this.setImage(context, 'images/states/pending');
                await this.setTitle(context, '');
                if (this.settings[context].spotify_url && (!badUrl))
                    this.#cachedPlayContexts[context] = await wrapper.getInformationOnUrl(this.settings[context].spotify_url);
                else
                    this.#cachedPlayContexts[context] = null;
            }
            if (this.#cachedPlayContexts[context]) {
                if (!images.isItemCached(this.#cachedPlayContexts[context]))
                    await this.setImage(context, 'images/states/pending');
                const image = await images.getForItem(this.#cachedPlayContexts[context]);
                if ((!this.marquees[context]) || this.marquees[context].id !== this.#cachedPlayContexts[context].url)
                    await this.marqueeTitle(this.#cachedPlayContexts[context].url, [
                        this.settings[context].show.includes('title') ? {
                            key: 'title',
                            value: this.#cachedPlayContexts[context].title
                        } : undefined,
                        this.#cachedPlayContexts[context].subtitle && this.settings[context].show.includes('subtitle') ? {
                            key: 'subtitle',
                            value: this.#cachedPlayContexts[context].subtitle
                        } : undefined,
                        this.#cachedPlayContexts[context].extra && this.settings[context].show.includes('extra') ? {
                            key: 'extra',
                            value: this.#cachedPlayContexts[context].extra
                        } : undefined
                    ].filter(v => !!v), context);
                else
                    this.resumeMarquee(context);
                const matchesActive = this.#cachedPlayContexts[context].type === 'track' ? wrapper.song?.item?.uri === this.#cachedPlayContexts[context].uri : wrapper.playbackContext?.uri === this.#cachedPlayContexts[context].uri;
                if (image)
                    await this.setImage(context, this.processImage(`data:image/jpeg;base64,${image}`, 'none', (this.settings[context].show.includes('active_border') && (matchesActive || this.#forcedActiveContexts[context] === this.#cachedPlayContexts[context].uri)) ? (matchesActive ? '#1db954' : '#dab824') : (this.settings[context].show.includes('inactive_border') ? '#888888' : null)));
                else if (this.#cachedPlayContexts[context].type === 'local')
                    await this.setImage(context, 'images/states/local');
                else
                    await this.setImage(context);
            }
            else
                await this.setImage(context, 'images/states/play-context-unknown');
            this.setUnpressable(context, false);
        }
        async invokeWrapperAction(context, type) {
            if (type === Button.TYPES.RELEASED)
                return;
            if (this.#cachedPlayContexts[context]) {
                const response = await wrapper.playItem(this.#cachedPlayContexts[context]);
                if (response === constants.WRAPPER_RESPONSE_SUCCESS) {
                    this.#forcedActiveContexts = {};
                    this.#forcedActiveContexts[context] = this.#cachedPlayContexts[context].uri;
                    for (const ctx of this.contexts)
                        setImmediate(() => this.#updatePlayContext(ctx, this.settings[ctx]));
                    return constants.WRAPPER_RESPONSE_SUCCESS_INDICATIVE;
                }
                else
                    return response;
            }
            else
                return constants.WRAPPER_RESPONSE_NOT_AVAILABLE;
        }
        async onSettingsUpdated(context, oldSettings) {
            await super.onSettingsUpdated(context, oldSettings);
            if (!this.settings[context].show)
                await this.setSettings(context, {
                    show: ['title', 'extra', 'subtitle', 'active_border', 'inactive_border']
                });
            if (this.#cachedPlayContexts[context]?.url !== this.settings[context].spotify_url || oldSettings.show?.length !== this.settings[context].show?.length || (oldSettings.show && this.settings[context].show && (!oldSettings.show.every((value, index) => value === this.settings[context].show[index]))))
                await this.#updatePlayContext(context, oldSettings);
        }
        async onWillDisappear(ev) {
            await super.onWillDisappear(ev);
            this.pauseMarquee(ev.action.id);
        }
        async onStateSettled(context) {
            await super.onStateSettled(context, true);
            await this.#updatePlayContext(context);
        }
        async onStateLoss(context) {
            await super.onStateLoss(context);
            this.clearMarquee(context);
            await this.setTitle(context, '');
        }
        static {
            __runInitializers(_classThis, _classExtraInitializers);
        }
    });
    return _classThis;
})();

let SongStackButton = (() => {
    let _classDecorators = [action({ UUID: 'com.ntanis.essentials-for-spotify.song-stack-button' })];
    let _classDescriptor;
    let _classExtraInitializers = [];
    let _classThis;
    let _classSuper = Button;
    (class extends _classSuper {
        static { _classThis = this; }
        static {
            const _metadata = typeof Symbol === "function" && Symbol.metadata ? Object.create(_classSuper[Symbol.metadata] ?? null) : void 0;
            __esDecorate(null, _classDescriptor = { value: _classThis }, _classDecorators, { kind: "class", name: _classThis.name, metadata: _metadata }, null, _classExtraInitializers);
            _classThis = _classDescriptor.value;
            if (_metadata) Object.defineProperty(_classThis, Symbol.metadata, { enumerable: true, configurable: true, writable: true, value: _metadata });
        }
        static STATABLE = true;
        static MULTI = true;
        constructor() {
            super();
            this.setStatelessImage('images/states/song-stack-unknown');
            wrapper.on('songChanged', this.#onSongChanged.bind(this));
            wrapper.on('songTimeChanged', this.#onSongTimeChanged.bind(this));
            wrapper.on('songLikedStateChanged', (liked, pending = false) => this.#onSongChanged(wrapper.song, wrapper.pendingSongChange));
        }
        #onSongTimeChanged(progress, duration, pending = false, contexts = this.contexts) {
            for (const context of contexts)
                if (this.marquees[context])
                    this.updateMarqueeEntry(context, 'time', this.beautifyTime(progress, duration, this.settings[context].show.includes('progress'), this.settings[context].show.includes('duration'), this.settings[context].time_display === 'remaining'));
        }
        async #onSongChanged(song, pending = false, contexts = this.contexts, force = false) {
            const promises = [];
            for (const context of contexts)
                promises.push(new Promise(async (resolve) => {
                    this.setUnpressable(context, true);
                    if ((song && this.marquees[context] && this.marquees[context].id !== song.item.id) || ((!song) && this.marquees[context]) || force) {
                        this.clearMarquee(context);
                        await this.setTitle(context, '');
                    }
                    if (song) {
                        if (!images.isSongCached(song))
                            await this.setImage(context, 'images/states/pending');
                        const image = await images.getForSong(song);
                        if ((!this.marquees[context]) || this.marquees[context].id !== song.item.id || force)
                            await this.marqueeTitle(song.item.id, [
                                this.settings[context].show.includes('name') ? {
                                    key: 'title',
                                    value: song.item.name
                                } : undefined,
                                this.settings[context].show.includes('artists') ? {
                                    key: 'artists',
                                    value: song.item.artists.map((artist) => artist.name).join(', ')
                                } : undefined,
                                this.settings[context].show.includes('progress') || this.settings[context].show.includes('duration') ? {
                                    key: 'time',
                                    value: this.beautifyTime(song.progress, song.item.duration_ms, this.settings[context].show.includes('progress'), this.settings[context].show.includes('duration'), this.settings[context].time_display === 'remaining')
                                } : undefined
                            ], context);
                        else
                            this.resumeMarquee(context);
                        if (image)
                            await this.setImage(context, this.processImage(`data:image/jpeg;base64,${image}`, this.settings[context].show.includes('liked') && song.liked ? 'center' : 'none'));
                        else if (song.item.uri.includes('local:'))
                            await this.setImage(context, 'images/states/local');
                        else
                            await this.setImage(context);
                    }
                    else if (pending)
                        await this.setImage(context, 'images/states/pending');
                    else
                        await this.setImage(context, 'images/states/song-stack-unknown');
                    if (!pending)
                        this.setUnpressable(context, false);
                    resolve(true);
                }));
            await Promise.allSettled(promises);
        }
        async #openSpotify() {
            if ((!wrapper.song) || wrapper.song.item.uri.includes('local:'))
                return constants.WRAPPER_RESPONSE_NOT_AVAILABLE;
            switch (os.platform()) {
                case 'darwin':
                    spawn('open', [wrapper.song.item.uri]);
                    break;
                case 'win32':
                    spawn('cmd', ['/c', 'start', '', wrapper.song.item.uri]);
                    break;
                case 'linux':
                    spawn('xdg-open', [wrapper.song.item.uri]);
                    break;
                default:
                    return constants.WRAPPER_RESPONSE_NOT_AVAILABLE;
            }
            return constants.WRAPPER_RESPONSE_SUCCESS_INDICATIVE;
        }
        async #invokePress(context, action) {
            switch (action) {
                case 'play_pause':
                    return wrapper.togglePlayback();
                case 'open_spotify':
                    return this.#openSpotify();
                case 'next_song':
                    return wrapper.nextSong();
                case 'previous_song':
                    return wrapper.previousSong();
                case 'like_unlike':
                    return wrapper.toggleCurrentSongLike();
            }
        }
        async invokeWrapperAction(context, type) {
            if (type === Button.TYPES.RELEASED)
                return;
            if (type === Button.TYPES.SINGLE_PRESS)
                return this.#invokePress(context, this.settings[context].single_press);
            else if (type === Button.TYPES.DOUBLE_PRESS)
                return this.#invokePress(context, this.settings[context].double_press);
            else if (type === Button.TYPES.TRIPLE_PRESS)
                return this.#invokePress(context, this.settings[context].triple_press);
            else if (type === Button.TYPES.LONG_PRESS)
                return this.#invokePress(context, this.settings[context].long_press);
            else
                return constants.WRAPPER_RESPONSE_NOT_AVAILABLE;
        }
        async onSettingsUpdated(context, oldSettings) {
            await super.onSettingsUpdated(context, oldSettings);
            if (!this.settings[context].single_press)
                await this.setSettings(context, {
                    single_press: 'play_pause'
                });
            if (!this.settings[context].double_press)
                await this.setSettings(context, {
                    double_press: 'next_song'
                });
            if (!this.settings[context].triple_press)
                await this.setSettings(context, {
                    triple_press: 'previous_song'
                });
            if (!this.settings[context].long_press)
                await this.setSettings(context, {
                    long_press: 'like_unlike'
                });
            if (!this.settings[context].show)
                await this.setSettings(context, {
                    show: ['name', 'artists', 'progress', 'duration', 'liked']
                });
            if (!this.settings[context].time_display)
                await this.setSettings(context, {
                    time_display: 'duration'
                });
            if (oldSettings.show?.length !== this.settings[context].show?.length || (oldSettings.show && this.settings[context].show && (!oldSettings.show.every((value, index) => value === this.settings[context].show[index]))) || oldSettings.time_display !== this.settings[context].time_display)
                await this.#onSongChanged(wrapper.song, wrapper.pendingSongChange, [context], true);
        }
        async onWillDisappear(ev) {
            await super.onWillDisappear(ev);
            this.pauseMarquee(ev.action.id);
        }
        async onStateSettled(context) {
            await super.onStateSettled(context, true);
            await this.#onSongChanged(wrapper.song, wrapper.pendingSongChange, [context]);
            this.#onSongTimeChanged(wrapper.song?.progress, wrapper.song?.item.duration_ms, wrapper.pendingSongChange, [context]);
        }
        async onStateLoss(context) {
            await super.onStateLoss(context);
            this.clearMarquee(context);
            await this.setTitle(context, '');
        }
        static {
            __runInitializers(_classThis, _classExtraInitializers);
        }
    });
    return _classThis;
})();

let SongClipboardButton = (() => {
    let _classDecorators = [action({ UUID: 'com.ntanis.essentials-for-spotify.song-clipboard-button' })];
    let _classDescriptor;
    let _classExtraInitializers = [];
    let _classThis;
    let _classSuper = Button;
    (class extends _classSuper {
        static { _classThis = this; }
        static {
            const _metadata = typeof Symbol === "function" && Symbol.metadata ? Object.create(_classSuper[Symbol.metadata] ?? null) : void 0;
            __esDecorate(null, _classDescriptor = { value: _classThis }, _classDecorators, { kind: "class", name: _classThis.name, metadata: _metadata }, null, _classExtraInitializers);
            _classThis = _classDescriptor.value;
            if (_metadata) Object.defineProperty(_classThis, Symbol.metadata, { enumerable: true, configurable: true, writable: true, value: _metadata });
            __runInitializers(_classThis, _classExtraInitializers);
        }
        #copyToClipboard(text) {
            let process = null;
            try {
                switch (os.platform()) {
                    case 'darwin':
                        process = spawn('pbcopy');
                        break;
                    case 'win32':
                        process = spawn('clip');
                        break;
                    case 'linux':
                        process = spawn('xclip', ['-selection', 'c']);
                        break;
                    default:
                        return constants.WRAPPER_RESPONSE_NOT_AVAILABLE;
                }
                process.stdin.end(text.replace(/[\x00-\x09\x0B-\x1F\x7F-\x9F]/g, ''));
            }
            catch (e) {
                logger.error(`An error occurred while copying to clipboard: "${e.message || 'No message.'}" @ "${e.stack || 'No stack trace.'}".`);
                return constants.WRAPPER_RESPONSE_FATAL_ERROR;
            }
            return constants.WRAPPER_RESPONSE_SUCCESS_INDICATIVE;
        }
        #getElements(context) {
            const settings = this.settings[context];
            if (settings.elements && Array.isArray(settings.elements))
                return settings.elements;
            return [
                {
                    key: 'title',
                    enabled: true
                },
                {
                    key: 'artists',
                    enabled: true
                },
                {
                    key: 'link',
                    enabled: true
                }
            ];
        }
        async onSettingsUpdated(context, oldSettings) {
            if (this.settings[context].separator === undefined)
                await this.setSettings(context, {
                    separator: ' - '
                });
        }
        async invokeWrapperAction(context, type) {
            if (type === Button.TYPES.RELEASED)
                return;
            if ((!wrapper.song) || wrapper.song.item.uri.includes('local:'))
                return constants.WRAPPER_RESPONSE_NOT_AVAILABLE;
            const elements = this.#getElements(context);
            const separator = this.settings[context].separator ?? ' - ';
            const segments = [];
            let currentParts = [];
            for (const element of elements) {
                if (!element.enabled)
                    continue;
                if (element.key === 'title')
                    currentParts.push(wrapper.song.item.name);
                else if (element.key === 'artists')
                    currentParts.push(wrapper.song.item.artists.map((a) => a.name).join(', '));
                else if (element.key === 'link') {
                    if (currentParts.length > 0) {
                        segments.push(currentParts.join(separator));
                        currentParts = [];
                    }
                    segments.push(wrapper.song.item.external_urls.spotify);
                }
            }
            if (currentParts.length > 0)
                segments.push(currentParts.join(separator));
            if (segments.length === 0)
                return constants.WRAPPER_RESPONSE_NOT_AVAILABLE;
            return this.#copyToClipboard(segments.join('\n'));
        }
    });
    return _classThis;
})();

let ContextInformationButton = (() => {
    let _classDecorators = [action({ UUID: 'com.ntanis.essentials-for-spotify.context-information-button' })];
    let _classDescriptor;
    let _classExtraInitializers = [];
    let _classThis;
    let _classSuper = Button;
    (class extends _classSuper {
        static { _classThis = this; }
        static {
            const _metadata = typeof Symbol === "function" && Symbol.metadata ? Object.create(_classSuper[Symbol.metadata] ?? null) : void 0;
            __esDecorate(null, _classDescriptor = { value: _classThis }, _classDecorators, { kind: "class", name: _classThis.name, metadata: _metadata }, null, _classExtraInitializers);
            _classThis = _classDescriptor.value;
            if (_metadata) Object.defineProperty(_classThis, Symbol.metadata, { enumerable: true, configurable: true, writable: true, value: _metadata });
        }
        static STATABLE = true;
        constructor() {
            super();
            this.setStatelessImage('images/states/context-information-unknown');
            wrapper.on('playbackContextChanged', this.#onPlaybackContextChanged.bind(this));
            wrapper.on('songChanged', this.#onSongChanged.bind(this));
        }
        async #onSongChanged(song, pending = false) {
            for (const context of this.contexts)
                if (this.settings[context]?.image_source === 'album' && wrapper.playbackContext)
                    await this.#updateImage(context, wrapper.playbackContext, song, pending);
        }
        async #updateImage(context, playbackContext, song, pending) {
            const imageSource = this.settings[context]?.image_source || 'context';
            if (imageSource === 'album' && song?.item?.album) {
                if (!images.isSongCached(song))
                    await this.setImage(context, 'images/states/pending');
                const image = await images.getForSong(song);
                if (image)
                    await this.setImage(context, `data:image/jpeg;base64,${image}`);
                else if (song.item.uri.includes('local:'))
                    await this.setImage(context, 'images/states/local');
                else
                    await this.setImage(context);
            }
            else if (playbackContext) {
                if (!images.isItemCached(playbackContext))
                    await this.setImage(context, 'images/states/pending');
                const image = await images.getForItem(playbackContext);
                if (image)
                    await this.setImage(context, `data:image/jpeg;base64,${image}`);
                else if (playbackContext.type === 'local')
                    await this.setImage(context, 'images/states/local');
                else
                    await this.setImage(context);
            }
            else if (pending)
                await this.setImage(context, 'images/states/pending');
            else
                await this.setImage(context, 'images/states/context-information-unknown');
        }
        async #onPlaybackContextChanged(playbackContext, pending = false, contexts = this.contexts, force = false) {
            const promises = [];
            for (const context of contexts)
                promises.push(new Promise(async (resolve) => {
                    this.setUnpressable(context, true);
                    if ((playbackContext && this.marquees[context] && this.marquees[context].id !== playbackContext.uri) || ((!playbackContext) && this.marquees[context]) || force) {
                        this.clearMarquee(context);
                        await this.setTitle(context, '');
                    }
                    if (playbackContext) {
                        if ((!this.marquees[context]) || this.marquees[context].id !== playbackContext.uri || force)
                            await this.marqueeTitle(playbackContext.uri, [
                                this.settings[context].show.includes('title') ? {
                                    key: 'title',
                                    value: playbackContext.title
                                } : undefined,
                                playbackContext.subtitle && this.settings[context].show.includes('subtitle') ? {
                                    key: 'subtitle',
                                    value: playbackContext.subtitle
                                } : undefined,
                                playbackContext.extra && this.settings[context].show.includes('extra') ? {
                                    key: 'extra',
                                    value: playbackContext.extra
                                } : undefined
                            ].filter(v => !!v), context);
                        else
                            this.resumeMarquee(context);
                        await this.#updateImage(context, playbackContext, wrapper.song, pending);
                    }
                    else if (pending)
                        await this.setImage(context, 'images/states/pending');
                    else
                        await this.setImage(context, 'images/states/context-information-unknown');
                    if (!pending)
                        this.setUnpressable(context, false);
                    resolve(true);
                }));
            await Promise.allSettled(promises);
        }
        async invokeWrapperAction(context, type) {
            if (type === Button.TYPES.RELEASED)
                return;
            if (this.settings[context].action === 'play_pause')
                return wrapper.togglePlayback();
            else if (this.settings[context].action === 'open_spotify')
                if (wrapper.playbackContext && wrapper.playbackContext.type !== 'local') {
                    switch (os.platform()) {
                        case 'darwin':
                            spawn('open', [wrapper.playbackContext.uri]);
                            break;
                        case 'win32':
                            spawn('cmd', ['/c', 'start', '', wrapper.playbackContext.uri]);
                            break;
                        case 'linux':
                            spawn('xdg-open', [wrapper.playbackContext.uri]);
                            break;
                        default:
                            return constants.WRAPPER_RESPONSE_NOT_AVAILABLE;
                    }
                    return constants.WRAPPER_RESPONSE_SUCCESS_INDICATIVE;
                }
            return constants.WRAPPER_RESPONSE_NOT_AVAILABLE;
        }
        async onSettingsUpdated(context, oldSettings) {
            await super.onSettingsUpdated(context, oldSettings);
            if (!this.settings[context].action)
                await this.setSettings(context, {
                    action: 'open_spotify'
                });
            if (!this.settings[context].show)
                await this.setSettings(context, {
                    show: ['title', 'extra', 'subtitle']
                });
            if (!this.settings[context].image_source)
                await this.setSettings(context, {
                    image_source: 'context'
                });
            if (oldSettings.show?.length !== this.settings[context].show?.length || (oldSettings.show && this.settings[context].show && (!oldSettings.show.every((value, index) => value === this.settings[context].show[index]))))
                await this.#onPlaybackContextChanged(wrapper.playbackContext, wrapper.pendingPlaybackContext, [context], true);
            if (oldSettings.image_source !== this.settings[context].image_source && wrapper.playbackContext)
                await this.#updateImage(context, wrapper.playbackContext, wrapper.song, wrapper.pendingPlaybackContext);
        }
        async onWillDisappear(ev) {
            await super.onWillDisappear(ev);
            this.pauseMarquee(ev.action.id);
        }
        async onStateSettled(context) {
            await super.onStateSettled(context, true);
            await this.#onPlaybackContextChanged(wrapper.playbackContext, wrapper.pendingPlaybackContext, [context]);
        }
        async onStateLoss(context) {
            await super.onStateLoss(context);
            this.clearMarquee(context);
            await this.setTitle(context, '');
        }
        static {
            __runInitializers(_classThis, _classExtraInitializers);
        }
    });
    return _classThis;
})();

let TransferPlaybackButton = (() => {
    let _classDecorators = [action({ UUID: 'com.ntanis.essentials-for-spotify.transfer-playback-button' })];
    let _classDescriptor;
    let _classExtraInitializers = [];
    let _classThis;
    let _classSuper = Button;
    (class extends _classSuper {
        static { _classThis = this; }
        static {
            const _metadata = typeof Symbol === "function" && Symbol.metadata ? Object.create(_classSuper[Symbol.metadata] ?? null) : void 0;
            __esDecorate(null, _classDescriptor = { value: _classThis }, _classDecorators, { kind: "class", name: _classThis.name, metadata: _metadata }, null, _classExtraInitializers);
            _classThis = _classDescriptor.value;
            if (_metadata) Object.defineProperty(_classThis, Symbol.metadata, { enumerable: true, configurable: true, writable: true, value: _metadata });
        }
        static STATABLE = true;
        constructor() {
            super();
            wrapper.on('devicesChanged', this.#updateDevices.bind(this));
            this.setStatelessImage('images/states/transfer-playback-unknown');
        }
        async #updateDevices(devices, contexts = this.contexts) {
            if (!connector.set)
                return;
            const promises = [];
            const items = [];
            for (const context of contexts)
                for (const device of devices)
                    if (device.id !== this.settings[context].spotify_device_id)
                        items.push({
                            value: device.id,
                            label: device.name
                        });
            for (const context of contexts)
                promises.push(new Promise(async (resolve) => {
                    const deviceOnline = devices.some((device) => device.id === this.settings[context].spotify_device_id);
                    if (deviceOnline)
                        await this.setSettings(context, {
                            spotify_device_label: (this.settings[context].spotify_device_id) ? (wrapper.devices.find((device) => device.id === this.settings[context].spotify_device_id)?.name) : undefined
                        });
                    if (this.settings[context].spotify_device_id)
                        items.unshift({
                            value: this.settings[context].spotify_device_id,
                            label: deviceOnline ? (this.settings[context].spotify_device_label ?? 'Unknown\nDevice') : (this.settings[context].spotify_device_label ? `${this.settings[context].spotify_device_label} (Offline)` : 'Unknown\nDevice (Offline)')
                        });
                    await streamDeck.ui.sendToPropertyInspector({
                        event: 'getDevices',
                        items
                    });
                    await this.setTitle(context, this.settings[context].spotify_device_label ? this.splitToLines(this.settings[context].spotify_device_label) : (this.settings[context].spotify_device_id ? 'Unknown\nDevice' : 'No Device\nSelected'));
                    if (deviceOnline)
                        await this.setImage(context, 'images/states/transfer-playback');
                    else
                        await this.setImage(context, 'images/states/transfer-playback-offline');
                    resolve(true);
                }));
            await Promise.allSettled(promises);
        }
        async invokeWrapperAction(context, type) {
            if (type === Button.TYPES.RELEASED)
                return;
            if ((!this.settings[context].spotify_device_id) || (!wrapper.devices.some((device) => device.id === this.settings[context].spotify_device_id)))
                return constants.WRAPPER_RESPONSE_NOT_AVAILABLE;
            else {
                const response = await wrapper.transferPlayback(this.settings[context].spotify_device_id);
                if (response === constants.WRAPPER_RESPONSE_SUCCESS)
                    return constants.WRAPPER_RESPONSE_SUCCESS_INDICATIVE;
                else
                    return response;
            }
        }
        async onSendToPlugin(ev) {
            if (ev.payload?.event === 'getDevices')
                await this.#updateDevices(wrapper.devices, [ev.action.id]);
        }
        async onSettingsUpdated(context, oldSettings) {
            await this.#updateDevices(wrapper.devices, [context]);
        }
        async onStateSettled(context) {
            await super.onStateSettled(context, true);
            await this.#updateDevices(wrapper.devices, [context]);
        }
        async onStateLoss(context) {
            await super.onStateLoss(context);
            await this.setTitle(context, '');
        }
        static {
            __runInitializers(_classThis, _classExtraInitializers);
        }
    });
    return _classThis;
})();

let UserInformationButton = (() => {
    let _classDecorators = [action({ UUID: 'com.ntanis.essentials-for-spotify.user-information-button' })];
    let _classDescriptor;
    let _classExtraInitializers = [];
    let _classThis;
    let _classSuper = Button;
    (class extends _classSuper {
        static { _classThis = this; }
        static {
            const _metadata = typeof Symbol === "function" && Symbol.metadata ? Object.create(_classSuper[Symbol.metadata] ?? null) : void 0;
            __esDecorate(null, _classDescriptor = { value: _classThis }, _classDecorators, { kind: "class", name: _classThis.name, metadata: _metadata }, null, _classExtraInitializers);
            _classThis = _classDescriptor.value;
            if (_metadata) Object.defineProperty(_classThis, Symbol.metadata, { enumerable: true, configurable: true, writable: true, value: _metadata });
        }
        static STATABLE = true;
        constructor() {
            super();
            wrapper.on('userChanged', this.#refreshUser.bind(this));
            this.setStatelessImage('images/states/user-information-unknown');
        }
        async #refreshUser(user, pending = false, contexts = this.contexts, force = false) {
            const promises = [];
            for (const context of contexts)
                promises.push(new Promise(async (resolve) => {
                    if ((!user) || typeof (user) !== 'object' || (this.marquees[context] && this.marquees[context].id !== user.id)) {
                        images.clearRaw('userProfilePicture');
                        this.clearMarquee(context);
                        await this.setTitle(context, '');
                    }
                    else if (force) {
                        this.clearMarquee(context);
                        await this.setTitle(context, '');
                    }
                    if (!images.isRawCached('userProfilePicture'))
                        await this.setImage(context, 'images/states/pending');
                    if ((!user) || typeof user !== 'object') {
                        if (!pending)
                            await this.setImage(context, 'images/states/user-information');
                        resolve(true);
                        return;
                    }
                    const imageUrl = user.images?.length > 0 ? user.images.sort((a, b) => a.width - b.width)[0]?.url : undefined;
                    const image = imageUrl ? await images.getRaw(imageUrl, 'userProfilePicture') : null;
                    if (!image)
                        await this.setImage(context, 'images/states/user-information');
                    else
                        await this.setImage(context, `data:image/jpeg;base64,${image}`);
                    if (this.settings[context].show?.includes('display_name'))
                        if ((!this.marquees[context]) || this.marquees[context].id !== user.id || force)
                            await this.marqueeTitle(user.id, [
                                {
                                    key: 'display_name',
                                    value: user.display_name || user.id
                                }
                            ], context);
                        else
                            this.resumeMarquee(context);
                    resolve(true);
                }));
            await Promise.allSettled(promises);
        }
        async onWillDisappear(ev) {
            await super.onWillDisappear(ev);
            this.pauseMarquee(ev.action.id);
        }
        async invokeWrapperAction(context, type) {
            if (type === Button.TYPES.RELEASED)
                return;
            await this.#refreshUser(null, true, [context]);
            return await wrapper.updateUser();
        }
        async onSettingsUpdated(context, oldSettings) {
            await super.onSettingsUpdated(context, oldSettings);
            if (!this.settings[context].show)
                await this.setSettings(context, {
                    show: ['display_name']
                });
            if (oldSettings.show?.length !== this.settings[context].show?.length || (oldSettings.show && this.settings[context].show && (!oldSettings.show.every((value, index) => value === this.settings[context].show[index]))))
                await this.#refreshUser(wrapper.user, false, [context], true);
        }
        async onStateSettled(context) {
            await super.onStateSettled(context, true);
            if (!wrapper.user)
                await wrapper.updateUser().then(() => this.#refreshUser(wrapper.user, false, [context]));
            else
                await this.#refreshUser(wrapper.user, false, [context]);
        }
        async onStateLoss(context) {
            await super.onStateLoss(context);
            this.clearMarquee(context);
            await this.setTitle(context, '');
        }
        static {
            __runInitializers(_classThis, _classExtraInitializers);
        }
    });
    return _classThis;
})();

var _a;
class Dial extends Action {
    static HOLDABLE = false;
    static STATABLE = false;
    static TYPES = {
        ROTATE_CLOCKWISE: Symbol('ROTATE_CLOCKWISE'),
        ROTATE_COUNTERCLOCKWISE: Symbol('ROTATE_COUNTERCLOCKWISE'),
        UP: Symbol('UP'),
        DOWN: Symbol('DOWN'),
        TAP: Symbol('TAP'),
        LONG_TAP: Symbol('LONG_TAP')
    };
    layout;
    #busy = {};
    #unpressable = {};
    #holding = {};
    #marquees = {};
    icon;
    originalIcon;
    contexts = [];
    constructor(layout, icon) {
        super();
        this.layout = layout;
        this.icon = icon;
        this.originalIcon = icon;
        connector.on('setupStateChanged', (state) => {
            if (this.constructor.STATABLE)
                if (!state)
                    for (const context of this.contexts)
                        this.onStateLoss(context);
                else
                    for (const context of this.contexts)
                        this.onStateSettled(context);
            if (!state)
                for (const context of this.contexts)
                    this.resetFeedbackLayout(context);
            else
                for (const context of this.contexts)
                    this.updateFeedback(context);
        });
        if (connector.set)
            for (const context of this.contexts)
                this.updateFeedback(context);
    }
    async #processAction(action, type) {
        if (this.#busy[action.id] || this.#unpressable[action.id] || (type === _a.TYPES.DOWN && this.#holding[action.id]) || (type === _a.TYPES.UP && this.constructor.HOLDABLE && (!this.#holding[action.id])))
            return;
        this.#busy[action.id] = true;
        if (type === _a.TYPES.UP)
            delete this.#holding[action.id];
        if (!connector.set)
            await this.flashIcon(action.id, 'images/icons/setup-error.png');
        else {
            const response = (this.constructor.HOLDABLE && (type === _a.TYPES.DOWN || type === _a.TYPES.UP)) ? (type === _a.TYPES.DOWN ? await this.invokeHoldWrapperAction(action.id) : await this.invokeHoldReleaseWrapperAction(action.id)) : await this.invokeWrapperAction(action.id, type);
            if ((response === constants.WRAPPER_RESPONSE_SUCCESS || response === constants.WRAPPER_RESPONSE_SUCCESS_INDICATIVE) && type === _a.TYPES.DOWN && this.constructor.HOLDABLE)
                this.#holding[action.id] = true;
            if (response === constants.WRAPPER_RESPONSE_SUCCESS_INDICATIVE)
                await this.flashIcon(action.id, 'images/icons/success.png', false);
            else if (response === constants.WRAPPER_RESPONSE_NOT_AVAILABLE)
                await this.flashIcon(action.id, 'images/icons/not-available.png');
            else if (response === constants.WRAPPER_RESPONSE_API_RATE_LIMITED)
                await this.flashIcon(action.id, 'images/icons/api-rate-limited.png');
            else if (response === constants.WRAPPER_RESPONSE_API_ERROR)
                await this.flashIcon(action.id, 'images/icons/api-error.png');
            else if (response === constants.WRAPPER_RESPONSE_FATAL_ERROR)
                await this.flashIcon(action.id, 'images/icons/fatal-error.png');
            else if (response === constants.WRAPPER_RESPONSE_NO_DEVICE_ERROR)
                await this.flashIcon(action.id, 'images/icons/no-device-error.png');
            else if (response === constants.WRAPPER_RESPONSE_BUSY)
                await this.flashIcon(action.id, 'images/icons/busy.png');
        }
        delete this.#busy[action.id];
    }
    getIconForStatus(status) {
        if (!connector.set)
            return 'images/icons/setup-error.png';
        else if (status === constants.WRAPPER_RESPONSE_SUCCESS)
            return 'images/icons/success.png';
        else if (status === constants.WRAPPER_RESPONSE_NOT_AVAILABLE)
            return 'images/icons/not-available.png';
        else if (status === constants.WRAPPER_RESPONSE_API_RATE_LIMITED)
            return 'images/icons/api-rate-limited.png';
        else if (status === constants.WRAPPER_RESPONSE_API_ERROR)
            return 'images/icons/api-error.png';
        else if (status === constants.WRAPPER_RESPONSE_FATAL_ERROR)
            return 'images/icons/fatal-error.png';
        else if (status === constants.WRAPPER_RESPONSE_NO_DEVICE_ERROR)
            return 'images/icons/no-device-error.png';
        else if (status === constants.WRAPPER_RESPONSE_BUSY)
            return 'images/icons/busy.png';
    }
    async flashIcon(context, icon, alert = true, duration = 500, times = 1) {
        for (let i = 0; i < times; i++) {
            if (alert)
                await this.showAlert(context).catch((e) => logger.error(`An error occurred while showing the Stream Deck alert of "${this.manifestId}": "${e.message || 'No message.'}" @ "${e.stack || 'No stack trace.'}".`));
            await this.setFeedback(context, {
                icon
            }, true).catch((e) => logger.error(`An error occurred while setting the Stream Deck feedback of "${this.manifestId}": "${e.message || 'No message.'}" @ "${e.stack || 'No stack trace.'}".`));
            await new Promise(resolve => setTimeout(resolve, duration));
            await this.setFeedback(context, {
                icon: connector.set ? this.icon : this.originalIcon
            }, true).catch((e) => logger.error(`An error occurred while setting the Stream Deck feedback of "${this.manifestId}": "${e.message || 'No message.'}" @ "${e.stack || 'No stack trace.'}".`));
            if (i + 1 < times)
                await new Promise(resolve => setTimeout(resolve, duration));
        }
    }
    async marquee(id, key, value, countable, visible, context) {
        const marqueeIdentifier = `${context}-${key}`;
        const isInitial = !this.#marquees[marqueeIdentifier];
        id = id ?? crypto.randomUUID();
        const marqueeData = this.#marquees[marqueeIdentifier] || {
            timeout: null,
            id,
            key,
            original: value,
            countable,
            render: `${value} | `,
            visible,
            frame: 0,
            context,
            last: null,
            totalFrames: null
        };
        if (this.#marquees[marqueeIdentifier] && this.#marquees[marqueeIdentifier].id !== id)
            return;
        this.#marquees[marqueeIdentifier] = marqueeData;
        if (marqueeData.totalFrames === null)
            marqueeData.totalFrames = marqueeData.render.length;
        if (marqueeData.last && countable.length > marqueeData.visible)
            while (marqueeData.last[1] === ' ') {
                marqueeData.last = marqueeData.last.substr(1);
                marqueeData.frame++;
                if (marqueeData.frame >= marqueeData.totalFrames) {
                    marqueeData.frame = 0;
                    marqueeData.last = marqueeData.original;
                }
            }
        marqueeData.last = countable.length > marqueeData.visible ? `${marqueeData.render.substr(marqueeData.frame, marqueeData.visible)}${marqueeData.frame + marqueeData.visible > marqueeData.render.length ? marqueeData.render.substr(0, (marqueeData.frame + marqueeData.visible) - marqueeData.render.length) : ''}` : marqueeData.original;
        await this.setFeedback(context, {
            [marqueeData.key]: marqueeData.last
        });
        if ((!this.#marquees[marqueeIdentifier]) || this.#marquees[marqueeIdentifier].id !== id)
            return;
        marqueeData.frame++;
        if (marqueeData.frame >= marqueeData.totalFrames)
            marqueeData.frame = 0;
        marqueeData.timeout = setTimeout(() => this.marquee(id, marqueeData.key, marqueeData.original, marqueeData.countable, marqueeData.visible, context), isInitial ? constants.DIAL_MARQUEE_INTERVAL_INITIAL : constants.DIAL_MARQUEE_INTERVAL);
    }
    async onDialRotate(ev) {
        return this.#processAction(ev.action, ev.payload.ticks > 0 ? _a.TYPES.ROTATE_CLOCKWISE : _a.TYPES.ROTATE_COUNTERCLOCKWISE);
    }
    async onDialUp(ev) {
        if (this.constructor.HOLDABLE)
            while (this.#busy[ev.action.id])
                await new Promise(resolve => setTimeout(resolve, 100));
        return this.#processAction(ev.action, _a.TYPES.UP);
    }
    async onDialDown(ev) {
        return this.#processAction(ev.action, _a.TYPES.DOWN);
    }
    async onTouchTap(ev) {
        return this.#processAction(ev.action, ev.payload.hold ? _a.TYPES.LONG_TAP : _a.TYPES.TAP);
    }
    async onWillAppear(ev) {
        await super.onWillAppear(ev);
        if (connector.set)
            await this.updateFeedback(ev.action.id);
    }
    async onWillDisappear(ev) {
        this.contexts.splice(this.contexts.indexOf(ev.action.id), 1);
    }
    async invokeWrapperAction(context, type) {
        return constants.WRAPPER_RESPONSE_NOT_AVAILABLE;
    }
    async invokeHoldWrapperAction(context) {
        if (this.constructor.HOLDABLE)
            return constants.WRAPPER_RESPONSE_NOT_AVAILABLE;
    }
    async invokeHoldReleaseWrapperAction(context) {
        if (this.constructor.HOLDABLE)
            return constants.WRAPPER_RESPONSE_NOT_AVAILABLE;
    }
    async setFeedback(context, feedback, force = false) {
        if (((!this.contexts.includes(context)) || (!connector.set)) && (!force))
            return;
        await this.actionObjects.get(context)?.setFeedback(feedback).catch((e) => logger.error(`An error occurred while setting the Stream Deck feedback of "${this.manifestId}": "${e.message || 'No message.'}" @ "${e.stack || 'No stack trace.'}".`));
    }
    async showAlert(context) {
        await this.actionObjects.get(context)?.showAlert().catch((e) => logger.error(`An error occurred while showing the Stream Deck alert of "${this.manifestId}": "${e.message || 'No message.'}" @ "${e.stack || 'No stack trace.'}".`));
    }
    async setIcon(context, icon, force = false) {
        if (((!this.contexts.includes(context)) || (!connector.set) && (!force)))
            return;
        this.icon = icon;
        await this.actionObjects.get(context)?.setFeedback({
            icon
        }).catch((e) => logger.error(`An error occurred while setting the Stream Deck feedback of "${this.manifestId}": "${e.message || 'No message.'}" @ "${e.stack || 'No stack trace.'}".`));
    }
    async resetFeedbackLayout(context, feedback = null) {
        for (const key in this.#marquees)
            this.pauseMarquee(context, this.#marquees[key].key);
        await this.actionObjects.get(context)?.setFeedbackLayout(this.layout).catch((e) => logger.error(`An error occurred while setting the Stream Deck feedback layout of "${this.manifestId}": "${e.message || 'No message.'}" @ "${e.stack || 'No stack trace.'}".`));
        if (feedback)
            await this.actionObjects.get(context)?.setFeedback(feedback).catch((e) => logger.error(`An error occurred while setting the Stream Deck feedback of "${this.manifestId}": "${e.message || 'No message.'}" @ "${e.stack || 'No stack trace.'}".`));
    }
    isHolding(context) {
        return this.#holding[context];
    }
    updateMarquee(context, key, value, countable) {
        const marqueeIdentifier = `${context}-${key}`;
        if (this.#marquees[marqueeIdentifier]) {
            this.#marquees[marqueeIdentifier].frame = 0;
            this.#marquees[marqueeIdentifier].original = value;
            this.#marquees[marqueeIdentifier].countable = countable;
            this.#marquees[marqueeIdentifier].render = `${value} | `;
            this.#marquees[marqueeIdentifier].totalFrames = this.#marquees[marqueeIdentifier].render.length;
        }
    }
    resumeMarquee(context, key) {
        const marqueeIdentifier = `${context}-${key}`;
        if (this.#marquees[marqueeIdentifier]) {
            this.#marquees[marqueeIdentifier].frame--;
            if (this.#marquees[marqueeIdentifier].frame < 0)
                this.#marquees[marqueeIdentifier].frame = this.#marquees[marqueeIdentifier].totalFrames;
            clearTimeout(this.#marquees[marqueeIdentifier].timeout);
            this.marquee(this.#marquees[marqueeIdentifier].id, this.#marquees[marqueeIdentifier].key, this.#marquees[marqueeIdentifier].original, this.#marquees[marqueeIdentifier].countable, this.#marquees[marqueeIdentifier].visible, context);
        }
    }
    pauseMarquee(context, key) {
        const marqueeIdentifier = `${context}-${key}`;
        if (this.#marquees[marqueeIdentifier]) {
            clearTimeout(this.#marquees[marqueeIdentifier].timeout);
            this.#marquees[marqueeIdentifier].timeout = null;
        }
    }
    clearMarquee(context, key) {
        const marqueeIdentifier = `${context}-${key}`;
        if (this.#marquees[marqueeIdentifier]) {
            clearTimeout(this.#marquees[marqueeIdentifier].timeout);
            delete this.#marquees[marqueeIdentifier];
        }
    }
    getMarquee(context, key) {
        return this.#marquees[`${context}-${key}`];
    }
    setUnpressable(context, busy) {
        if (!busy)
            delete this.#unpressable[context];
        else
            this.#unpressable[context] = busy;
    }
    async onStateSettled(context) { }
    async onStateLoss(context) { }
    async updateFeedback(context) { }
}
_a = Dial;

let VolumeControlDial = (() => {
    let _classDecorators = [action({ UUID: 'com.ntanis.essentials-for-spotify.volume-control-dial' })];
    let _classDescriptor;
    let _classExtraInitializers = [];
    let _classThis;
    let _classSuper = Dial;
    (class extends _classSuper {
        static { _classThis = this; }
        static {
            const _metadata = typeof Symbol === "function" && Symbol.metadata ? Object.create(_classSuper[Symbol.metadata] ?? null) : void 0;
            __esDecorate(null, _classDescriptor = { value: _classThis }, _classDecorators, { kind: "class", name: _classThis.name, metadata: _metadata }, null, _classExtraInitializers);
            _classThis = _classDescriptor.value;
            if (_metadata) Object.defineProperty(_classThis, Symbol.metadata, { enumerable: true, configurable: true, writable: true, value: _metadata });
        }
        static HOLDABLE = true;
        constructor() {
            super('layouts/volume-control-layout.json', 'images/icons/volume-control.png');
            wrapper.on('mutedStateChanged', this.#onMutedStateChanged.bind(this));
            wrapper.on('volumePercentChanged', this.#onVolumePercentChanged.bind(this));
            wrapper.on('deviceChanged', this.#onDeviceChanged.bind(this));
        }
        async #updateJointFeedback(contexts = this.contexts) {
            const promises = [];
            if (wrapper.volumePercent === null) {
                for (const context of contexts)
                    promises.push(this.resetFeedbackLayout(context));
                await Promise.allSettled(promises);
                return;
            }
            for (const context of contexts)
                promises.push(new Promise(async (resolve) => {
                    await this.setIcon(context, wrapper.muted ? 'images/icons/volume-control-muted.png' : 'images/icons/volume-control.png');
                    await this.setFeedback(context, {
                        text: {
                            value: `${wrapper.muted ? wrapper.mutedVolumePercent : wrapper.volumePercent}%`,
                            opacity: wrapper.muted ? 0.5 : 1.0
                        },
                        icon: {
                            opacity: 1
                        },
                        indicator: {
                            value: wrapper.muted ? wrapper.mutedVolumePercent : wrapper.volumePercent,
                            opacity: wrapper.muted ? 0.5 : 1.0
                        }
                    });
                    resolve(true);
                }));
            await Promise.allSettled(promises);
        }
        async #onVolumePercentChanged(percent, contexts = this.contexts) {
            await this.#updateJointFeedback(contexts);
        }
        async #onMutedStateChanged(state, contexts = this.contexts) {
            await this.#updateJointFeedback(contexts);
        }
        async #onDeviceChanged(device, contexts = this.contexts) {
            const promises = [];
            if (!device) {
                for (const context of contexts)
                    promises.push(this.resetFeedbackLayout(context));
                await Promise.allSettled(promises);
                return;
            }
            for (const context of contexts)
                promises.push(this.#updateJointFeedback([context]));
            await Promise.allSettled(promises);
        }
        async invokeWrapperAction(context, type) {
            if (type === Dial.TYPES.ROTATE_CLOCKWISE) {
                if (wrapper.volumePercent === null)
                    return constants.WRAPPER_RESPONSE_NOT_AVAILABLE;
                return wrapper.volumeUp(this.settings[context].step);
            }
            else if (type === Dial.TYPES.ROTATE_COUNTERCLOCKWISE)
                if (wrapper.volumePercent === null)
                    return constants.WRAPPER_RESPONSE_NOT_AVAILABLE;
                else
                    return wrapper.volumeDown(this.settings[context].step);
            else if (type === Dial.TYPES.TAP)
                return wrapper.toggleVolumeMute();
            else
                return constants.WRAPPER_RESPONSE_NOT_AVAILABLE;
        }
        async invokeHoldWrapperAction(context) {
            if (wrapper.volumePercent === null)
                return constants.WRAPPER_RESPONSE_NOT_AVAILABLE;
            else if (!wrapper.muted)
                return wrapper.muteVolume();
            else
                return constants.WRAPPER_RESPONSE_SUCCESS;
        }
        async invokeHoldReleaseWrapperAction(context) {
            if (wrapper.volumePercent === null)
                return constants.WRAPPER_RESPONSE_NOT_AVAILABLE;
            else if (wrapper.muted)
                return wrapper.unmuteVolume();
            else
                return constants.WRAPPER_RESPONSE_SUCCESS;
        }
        async resetFeedbackLayout(context) {
            await super.resetFeedbackLayout(context, {
                icon: this.originalIcon
            });
        }
        async onSettingsUpdated(context, oldSettings) {
            await super.onSettingsUpdated(context, oldSettings);
            if (!this.settings[context].step)
                await this.setSettings(context, {
                    step: constants.DEFAULT_VOLUME_STEP
                });
        }
        async updateFeedback(context) {
            await super.updateFeedback(context);
            await this.#onMutedStateChanged(wrapper.muted, [context]);
            await this.#onVolumePercentChanged(wrapper.volumePercent, [context]);
            await this.#onDeviceChanged(wrapper.device, [context]);
        }
        static {
            __runInitializers(_classThis, _classExtraInitializers);
        }
    });
    return _classThis;
})();

let PlaybackControlDial = (() => {
    let _classDecorators = [action({ UUID: 'com.ntanis.essentials-for-spotify.playback-control-dial' })];
    let _classDescriptor;
    let _classExtraInitializers = [];
    let _classThis;
    let _classSuper = Dial;
    (class extends _classSuper {
        static { _classThis = this; }
        static {
            const _metadata = typeof Symbol === "function" && Symbol.metadata ? Object.create(_classSuper[Symbol.metadata] ?? null) : void 0;
            __esDecorate(null, _classDescriptor = { value: _classThis }, _classDecorators, { kind: "class", name: _classThis.name, metadata: _metadata }, null, _classExtraInitializers);
            _classThis = _classDescriptor.value;
            if (_metadata) Object.defineProperty(_classThis, Symbol.metadata, { enumerable: true, configurable: true, writable: true, value: _metadata });
        }
        static HOLDABLE = true;
        #seeking = false;
        constructor() {
            super('layouts/playback-control-layout.json', 'images/icons/playback-control.png');
            wrapper.on('songChanged', this.#onSongChanged.bind(this));
            wrapper.on('songTimeChanged', this.#onSongTimeChanged.bind(this));
            wrapper.on('playbackStateChanged', this.#onPlaybackStateChanged.bind(this));
            wrapper.on('deviceChanged', this.#onDeviceChanged.bind(this));
            wrapper.on('songLikedStateChanged', (liked, pending = false) => this.#onSongChanged(wrapper.song, wrapper.pendingSongChange));
        }
        async #updateJointFeedback(contexts = this.contexts) {
            if (!wrapper.device)
                return;
            for (const context of contexts) {
                const titleMarquee = this.getMarquee(context, 'title');
                const timeMarquee = this.getMarquee(context, 'time');
                this.setFeedback(context, {
                    title: titleMarquee ? titleMarquee.last : 'Playback Control',
                    indicator: {
                        value: wrapper.song ? Math.round((wrapper.song.progress / wrapper.song.item.duration_ms) * 100) : 0,
                        opacity: wrapper.playing ? 1.0 : 0.5
                    },
                    icon: {
                        opacity: wrapper.song || wrapper.pendingSongChange ? 1.0 : 0.5
                    },
                    text: {
                        value: timeMarquee ? timeMarquee.last : `${this.settings[context].show.includes('progress') ? '??:??' : ''}${this.settings[context].show.includes('progress') && this.settings[context].show.includes('duration') ? ' / ' : ''}${this.settings[context].show.includes('duration') ? '??:??' : ''}`,
                        opacity: wrapper.playing ? 1.0 : 0.5
                    }
                });
            }
        }
        async #onSongChanged(song, pending = false, contexts = this.contexts, force = false) {
            const promises = [];
            for (const context of contexts)
                promises.push(new Promise(async (resolve) => {
                    this.setUnpressable(context, true);
                    let titleMarquee = this.getMarquee(context, 'title');
                    let timeMarquee = this.getMarquee(context, 'time');
                    await this.setFeedback(context, {
                        icon: {
                            opacity: wrapper.device && (wrapper.song || wrapper.pendingSongChange) ? 1.0 : 0.5
                        }
                    });
                    if (pending || (song && ((titleMarquee && titleMarquee.id !== song.item.id) || (timeMarquee && timeMarquee.id !== song.item.id))) || ((!song) && titleMarquee && timeMarquee) || force) {
                        this.clearMarquee(context, 'title');
                        this.clearMarquee(context, 'time');
                        titleMarquee = null;
                        timeMarquee = null;
                        if ((!force) || (!song))
                            await this.setFeedback(context, {
                                title: 'Playback Control',
                                text: {
                                    value: '??:?? / ??:??',
                                    opacity: wrapper.playing ? 1.0 : 0.5
                                }
                            });
                    }
                    if (song) {
                        if (!images.isSongCached(song))
                            await this.setIcon(context, 'images/icons/pending.png');
                        const image = await images.getForSong(song);
                        const time = this.beautifyTime(song.progress, song.item.duration_ms, this.settings[context].show.includes('progress'), this.settings[context].show.includes('duration'), this.settings[context].time_display === 'remaining');
                        await this.setFeedback(context, {
                            text: {
                                value: time
                            }
                        });
                        if ((!titleMarquee) || titleMarquee.id !== song.item.id || force) {
                            const title = `${this.settings[context].show.includes('name') ? song.item.name : ''}${this.settings[context].show.includes('name') && this.settings[context].show.includes('artists') ? ' - ' : ''}${this.settings[context].show.includes('artists') ? song.item.artists.map((artist) => artist.name).join(', ') : ''}`;
                            await this.marquee(undefined, 'title', title, title, 16, context);
                        }
                        else
                            this.resumeMarquee(context, 'title');
                        if ((!timeMarquee) || timeMarquee.id !== song.item.id || force)
                            await this.marquee(undefined, 'time', time === '' ? ' ' : time, time === '' ? ' ' : `${'8'.repeat(time.length - (this.settings[context].show.includes('progress') && this.settings[context].show.includes('duration') ? 5 : 1))}${this.settings[context].show.includes('progress') && this.settings[context].show.includes('duration') ? ': : /' : ':'}`, 14, context);
                        else
                            this.resumeMarquee(context, 'time');
                        if (image)
                            await this.setIcon(context, this.processImage(`data:image/jpeg;base64,${image}`, this.settings[context].show.includes('liked') && song.liked ? 'top-left' : 'none'));
                        else if (song.item.uri.includes('local:'))
                            await this.setIcon(context, 'images/states/local');
                        else
                            await this.setIcon(context, this.originalIcon);
                    }
                    else if (wrapper.pendingSongChange)
                        await this.setIcon(context, 'images/icons/pending.png');
                    else
                        await this.setIcon(context, this.originalIcon);
                    this.setUnpressable(context, false);
                    resolve(true);
                }));
            await Promise.allSettled(promises);
        }
        async #onSongTimeChanged(progress, duration, pending = false, contexts = this.contexts) {
            const promises = [];
            for (const context of contexts) {
                const timeMarquee = this.getMarquee(context, 'time');
                if (timeMarquee) {
                    const time = this.beautifyTime(progress, duration, this.settings[context].show.includes('progress'), this.settings[context].show.includes('duration'), this.settings[context].time_display === 'remaining');
                    this.updateMarquee(context, 'time', time === '' ? ' ' : time, time === '' ? ' ' : `${'8'.repeat(time.length - (this.settings[context].show.includes('progress') && this.settings[context].show.includes('duration') ? 5 : 1))}${this.settings[context].show.includes('progress') && this.settings[context].show.includes('duration') ? ': : /' : ':'}`);
                }
                promises.push(this.#updateJointFeedback([context]));
            }
            await Promise.allSettled(promises);
        }
        async #onPlaybackStateChanged(state, contexts = this.contexts) {
            const promises = [];
            for (const context of contexts)
                promises.push(this.#updateJointFeedback([context]));
            await Promise.allSettled(promises);
        }
        async #onDeviceChanged(device, contexts = this.contexts) {
            const promises = [];
            if (!device) {
                for (const context of contexts)
                    promises.push(this.resetFeedbackLayout(context));
                await Promise.allSettled(promises);
                return;
            }
            for (const context of contexts)
                promises.push(this.#updateJointFeedback([context]));
            await Promise.allSettled(promises);
        }
        async invokeWrapperAction(context, type) {
            if (type === Dial.TYPES.ROTATE_CLOCKWISE) {
                if (this.isHolding(context))
                    this.#seeking = true;
                else
                    this.#seeking = false;
                if ((!wrapper.song) && wrapper.pendingSongChange)
                    return constants.WRAPPER_RESPONSE_BUSY;
                else if (!this.isHolding(context))
                    return wrapper.nextSong();
                else if (wrapper.song && wrapper.song.progress + (this.settings[context].step ?? constants.DEFAULT_SEEK_STEP_SIZE) < wrapper.song.item.duration_ms)
                    return wrapper.forwardSeek(this.settings[context].step ?? constants.DEFAULT_SEEK_STEP_SIZE);
                else
                    return constants.WRAPPER_RESPONSE_NOT_AVAILABLE;
            }
            else if (type === Dial.TYPES.ROTATE_COUNTERCLOCKWISE) {
                if (this.isHolding(context))
                    this.#seeking = true;
                else
                    this.#seeking = false;
                if ((!wrapper.song) && wrapper.pendingSongChange)
                    return constants.WRAPPER_RESPONSE_BUSY;
                else if (!this.isHolding(context))
                    return wrapper.previousSong();
                else if (wrapper.song)
                    return wrapper.backwardSeek(this.settings[context].step ?? constants.DEFAULT_SEEK_STEP_SIZE);
                else
                    return constants.WRAPPER_RESPONSE_NOT_AVAILABLE;
            }
            else if (type === Dial.TYPES.TAP)
                return wrapper.togglePlayback();
            else if (type === Dial.TYPES.LONG_TAP)
                return wrapper.toggleCurrentSongLike();
            else
                return constants.WRAPPER_RESPONSE_NOT_AVAILABLE;
        }
        async invokeHoldWrapperAction(context) {
            return constants.WRAPPER_RESPONSE_SUCCESS;
        }
        async invokeHoldReleaseWrapperAction(context) {
            if (this.#seeking) {
                this.#seeking = false;
                return constants.WRAPPER_RESPONSE_SUCCESS;
            }
            if (wrapper.playing)
                return wrapper.pausePlayback();
            else
                return wrapper.resumePlayback();
        }
        async onWillDisappear(ev) {
            await super.onWillDisappear(ev);
            this.pauseMarquee(ev.action.id, 'title');
            this.pauseMarquee(ev.action.id, 'time');
        }
        async resetFeedbackLayout(context) {
            await super.resetFeedbackLayout(context, {
                title: 'Playback Control',
                icon: this.originalIcon
            });
        }
        async onSettingsUpdated(context, oldSettings) {
            await super.onSettingsUpdated(context, oldSettings);
            if (!this.settings[context].show)
                await this.setSettings(context, {
                    show: ['name', 'artists', 'progress', 'duration', 'liked']
                });
            if (!this.settings[context].step)
                await this.setSettings(context, {
                    step: constants.DEFAULT_SEEK_STEP_SIZE
                });
            if (!this.settings[context].time_display)
                await this.setSettings(context, {
                    time_display: 'duration'
                });
            if (oldSettings.show?.length !== this.settings[context].show?.length || (oldSettings.show && this.settings[context].show && (!oldSettings.show.every((value, index) => value === this.settings[context].show[index]))) || oldSettings.time_display !== this.settings[context].time_display)
                await this.#onSongChanged(wrapper.song, wrapper.pendingSongChange, [context], true);
        }
        async onStateLoss(context) {
            await super.onStateLoss(context);
            this.clearMarquee(context, 'title');
            this.clearMarquee(context, 'time');
        }
        async updateFeedback(context) {
            await super.updateFeedback(context);
            await this.#onSongChanged(wrapper.song, false, [context]);
            await this.#onSongTimeChanged(wrapper.song?.progress, wrapper.song?.item.duration_ms, false, [context]);
            await this.#onPlaybackStateChanged(wrapper.playing, [context]);
            await this.#onDeviceChanged(wrapper.device, [context]);
        }
        static {
            __runInitializers(_classThis, _classExtraInitializers);
        }
    });
    return _classThis;
})();

let ItemsDial = (() => {
    let _classDecorators = [action({ UUID: 'com.ntanis.essentials-for-spotify.items-dial' })];
    let _classDescriptor;
    let _classExtraInitializers = [];
    let _classThis;
    let _classSuper = Dial;
    (class extends _classSuper {
        static { _classThis = this; }
        static {
            const _metadata = typeof Symbol === "function" && Symbol.metadata ? Object.create(_classSuper[Symbol.metadata] ?? null) : void 0;
            __esDecorate(null, _classDescriptor = { value: _classThis }, _classDecorators, { kind: "class", name: _classThis.name, metadata: _metadata }, null, _classExtraInitializers);
            _classThis = _classDescriptor.value;
            if (_metadata) Object.defineProperty(_classThis, Symbol.metadata, { enumerable: true, configurable: true, writable: true, value: _metadata });
            __runInitializers(_classThis, _classExtraInitializers);
        }
        #currentItems = {};
        #itemsPage = {};
        #items = [];
        #lastTotal = 0;
        async #refreshItems(context) {
            if (this.#itemsPage[context] === undefined)
                this.#itemsPage[context] = 1;
            const apiCall = await this.fetchItems(this.#itemsPage[context], context);
            if ((!apiCall) || typeof apiCall !== 'object' || (apiCall.status !== constants.WRAPPER_RESPONSE_SUCCESS && apiCall.status !== constants.WRAPPER_RESPONSE_SUCCESS_INDICATIVE)) {
                this.resetFeedbackLayout(context, {
                    name: {
                        opacity: 1.0
                    },
                    icon: {
                        opacity: 1.0
                    },
                    count: {
                        opacity: 1.0
                    },
                    extra: {
                        opacity: 1.0
                    }
                });
                const icon = this.getIconForStatus(typeof apiCall === 'object' ? apiCall.status : apiCall);
                if (icon)
                    await this.flashIcon(context, icon);
                return;
            }
            const lastTotal = this.#lastTotal;
            delete apiCall.status;
            this.#items = apiCall;
            this.#lastTotal = this.#items.total <= constants.WRAPPER_ITEMS_PER_PAGE ? this.#items.items.length : this.#items.total;
            return lastTotal !== this.#lastTotal;
        }
        async #refreshPage(context) {
            const nameMarquee = this.getMarquee(context, 'name');
            if (nameMarquee)
                this.pauseMarquee(context, 'name');
            this.setIcon(context, 'images/icons/pending.png');
            this.#refreshCount(context);
            const refreshItems = await this.#refreshItems(context);
            if (refreshItems) {
                await this.#refreshLayout(true, context);
                return false;
            }
            return true;
        }
        async #refreshLayout(refreshItems = false, context) {
            const nameMarquee = this.getMarquee(context, 'name');
            if (refreshItems) {
                await this.resetFeedbackLayout(context, {
                    name: {
                        opacity: 1.0
                    },
                    icon: {
                        opacity: 1.0
                    },
                    count: {
                        opacity: 1.0
                    },
                    opacity: {
                        opacity: 1.0
                    }
                });
                await this.setIcon(context, 'images/icons/pending.png');
                this.#itemsPage = {};
                this.#currentItems = {};
                this.#items = {};
                const refreshResult = await this.#refreshItems(context);
                if (refreshResult === undefined) {
                    await this.setIcon(context, this.originalIcon);
                    return;
                }
                if (this.#lastTotal === 0) {
                    await this.setIcon(context, this.originalIcon);
                    await this.setFeedback(context, {
                        count: {
                            value: '0 / 0'
                        }
                    });
                    return;
                }
                if (this.#currentItems[context] === undefined)
                    this.#currentItems[context] = 0;
            }
            if (!this.#items.items[this.#currentItems[context]])
                if (!refreshItems)
                    return this.#refreshLayout(true, context);
            if (!images.isItemCached(this.#items.items[this.#currentItems[context]]))
                await this.setIcon(context, 'images/icons/pending.png');
            await this.setFeedback(context, {
                name: {
                    opacity: 1.0
                },
                extra: {
                    opacity: 1.0,
                    value: this.#items.items[this.#currentItems[context]].extra ?? ''
                },
                icon: {
                    opacity: 1.0
                },
                count: {
                    opacity: 1.0
                }
            });
            await this.#refreshCount(context);
            if (nameMarquee) {
                if (nameMarquee.original !== this.#items.items[this.#currentItems[context]].name)
                    this.updateMarquee(context, 'name', this.#items.items[this.#currentItems[context]].name, this.#items.items[this.#currentItems[context]].name);
                this.resumeMarquee(context, 'name');
            }
            else
                await this.marquee(undefined, 'name', this.#items.items[this.#currentItems[context]].name, this.#items.items[this.#currentItems[context]].name, 11, context);
            const image = await images.getForItem(this.#items.items[this.#currentItems[context]]);
            if (image)
                await this.setIcon(context, `data:image/jpeg;base64,${image}`);
            else
                await this.setIcon(context, this.originalIcon);
        }
        async #refreshCount(context) {
            await this.setFeedback(context, {
                count: {
                    value: `${((this.#itemsPage[context] - 1) * constants.WRAPPER_ITEMS_PER_PAGE) + this.#currentItems[context] + 1} / ${this.#lastTotal}`
                }
            });
        }
        async softRefresh(context) {
            await this.#refreshItems(context);
            await this.#refreshLayout(false, context);
        }
        async invokeWrapperAction(context, type) {
            if (type === Dial.TYPES.ROTATE_CLOCKWISE) {
                if (this.#lastTotal <= 1)
                    return constants.WRAPPER_RESPONSE_SUCCESS;
                this.pauseMarquee(context, 'name');
                this.#currentItems[context]++;
                if (this.#currentItems[context] >= this.#items.items.length) {
                    this.#currentItems[context] = 0;
                    const lastPage = this.#itemsPage[context];
                    if (this.#itemsPage[context] < Math.ceil(this.#lastTotal / constants.WRAPPER_ITEMS_PER_PAGE))
                        this.#itemsPage[context]++;
                    else
                        this.#itemsPage[context] = 1;
                    if (lastPage !== this.#itemsPage[context]) {
                        await this.setFeedback(context, {
                            name: {
                                value: '?????'
                            }
                        });
                        if (!(await this.#refreshPage(context)))
                            return constants.WRAPPER_RESPONSE_API_ERROR;
                    }
                }
                await this.#refreshCount(context);
                await this.#refreshLayout(false, context);
            }
            else if (type === Dial.TYPES.ROTATE_COUNTERCLOCKWISE) {
                if (this.#lastTotal <= 1)
                    return constants.WRAPPER_RESPONSE_SUCCESS;
                this.pauseMarquee(context, 'name');
                this.#currentItems[context]--;
                if (this.#currentItems[context] < 0) {
                    const lastPage = this.#itemsPage[context];
                    if (this.#itemsPage[context] > 1) {
                        this.#itemsPage[context]--;
                        this.#currentItems[context] = constants.WRAPPER_ITEMS_PER_PAGE - 1;
                    }
                    else {
                        this.#itemsPage[context] = Math.ceil(this.#lastTotal / constants.WRAPPER_ITEMS_PER_PAGE);
                        this.#currentItems[context] = this.#lastTotal - ((this.#itemsPage[context] - 1) * constants.WRAPPER_ITEMS_PER_PAGE) - 1;
                    }
                    if (lastPage !== this.#itemsPage[context]) {
                        await this.setFeedback(context, {
                            name: {
                                value: '?????'
                            }
                        });
                        if (!(await this.#refreshPage(context)))
                            return constants.WRAPPER_RESPONSE_API_ERROR;
                    }
                }
                await this.#refreshCount(context);
                await this.#refreshLayout(false, context);
            }
            else if (type === Dial.TYPES.LONG_TAP) {
                await this.#refreshLayout(true, context);
                return constants.WRAPPER_RESPONSE_SUCCESS;
            }
            else if (type === Dial.TYPES.DOWN)
                return constants.WRAPPER_RESPONSE_SUCCESS;
            else if (type === Dial.TYPES.UP || type === Dial.TYPES.TAP) {
                if (this.#lastTotal === 0)
                    return constants.WRAPPER_RESPONSE_NOT_AVAILABLE;
                if (this.#currentItems[context] !== undefined) {
                    if (this.#items.items[this.#currentItems[context]]) {
                        const apiCall = await this.playSelectedItem(this.#items.items[this.#currentItems[context]]);
                        if (apiCall !== constants.WRAPPER_RESPONSE_SUCCESS && apiCall !== constants.WRAPPER_RESPONSE_SUCCESS_INDICATIVE) {
                            await this.#refreshLayout(true, context);
                            return apiCall;
                        }
                        return constants.WRAPPER_RESPONSE_SUCCESS;
                    }
                    await this.#refreshLayout(true, context);
                }
                return constants.WRAPPER_RESPONSE_API_ERROR;
            }
            else
                return constants.WRAPPER_RESPONSE_NOT_AVAILABLE;
        }
        async resetFeedbackLayout(context, feedback = {}) {
            await super.resetFeedbackLayout(context, Object.assign({
                icon: this.originalIcon
            }, feedback));
        }
        async playSelectedItem(item) {
            return wrapper.playItem(item);
        }
        async fetchItems(page, context) {
            throw new Error('The fetchItems method must be implemented in a subclass.');
        }
        async onWillDisappear(ev) {
            await super.onWillDisappear(ev);
            this.pauseMarquee(ev.action.id, 'name');
        }
        async updateFeedback(context) {
            await super.updateFeedback(context);
            await this.#refreshLayout(this.#lastTotal === 0, context);
        }
    });
    return _classThis;
})();

const PLAYLIST_URL_REGEX = /^https?:\/\/open\.spotify\.com\/(?:intl-[a-z]{2}\/)?playlist\/([A-Za-z0-9]{22})(?:\/)?(?:\?.*)?$/i;
const LIKED_SONGS_URL_REGEX = /^https?:\/\/open\.spotify\.com\/(?:intl-[a-z]{2}\/)?collection\/tracks(?:\/)?(?:\?.*)?$/i;
let MyPlaylistsDial = (() => {
    let _classDecorators = [action({ UUID: 'com.ntanis.essentials-for-spotify.my-playlists-dial' })];
    let _classDescriptor;
    let _classExtraInitializers = [];
    let _classThis;
    let _classSuper = ItemsDial;
    (class extends _classSuper {
        static { _classThis = this; }
        static {
            const _metadata = typeof Symbol === "function" && Symbol.metadata ? Object.create(_classSuper[Symbol.metadata] ?? null) : void 0;
            __esDecorate(null, _classDescriptor = { value: _classThis }, _classDecorators, { kind: "class", name: _classThis.name, metadata: _metadata }, null, _classExtraInitializers);
            _classThis = _classDescriptor.value;
            if (_metadata) Object.defineProperty(_classThis, Symbol.metadata, { enumerable: true, configurable: true, writable: true, value: _metadata });
            __runInitializers(_classThis, _classExtraInitializers);
        }
        #extraEntries = {};
        #resolvedExtras = {};
        #lastSpotifyTotal = 0;
        constructor() {
            super('layouts/items-layout.json', 'images/icons/playlists.png');
        }
        async #buildExtras(context, entries) {
            this.#resolvedExtras[context] = entries.filter(entry => entry.url).map(entry => {
                const playlistMatch = entry.url.match(PLAYLIST_URL_REGEX);
                if (playlistMatch)
                    return {
                        id: playlistMatch[1],
                        type: 'playlist',
                        name: entry.name || '',
                        images: []
                    };
                else if (wrapper.user?.id && LIKED_SONGS_URL_REGEX.test(entry.url))
                    return {
                        id: `${wrapper.user.id}:collection`,
                        type: 'user',
                        name: entry.name || 'Liked Songs',
                        images: [{
                                width: 64,
                                height: 64,
                                url: 'https://misc.scdn.co/liked-songs/liked-songs-64.jpg'
                            }]
                    };
                return null;
            }).filter(v => !!v);
            for (const extra of this.#resolvedExtras[context])
                if (extra.type === 'playlist') {
                    const oembed = await wrapper.getOembed(extra.id);
                    if (oembed) {
                        if (!extra.name && oembed.title)
                            extra.name = oembed.title;
                        if (extra.images.length === 0 && oembed.thumbnailUrl)
                            extra.images = [{ url: oembed.thumbnailUrl }];
                    }
                }
        }
        async #refreshResolvedExtras(context) {
            await this.#buildExtras(context, this.#extraEntries[context] || []);
            wrapper.setKnownPlaylists(Object.values(this.#resolvedExtras).flat().filter(entry => entry.type === 'playlist'));
        }
        async onSettingsUpdated(context, _oldSettings) {
            this.#extraEntries[context] = this.settings[context].extra_playlists || [];
            const previousIds = (this.#resolvedExtras[context] || []).map((e) => e.id).join(',');
            await this.#refreshResolvedExtras(context);
            const currentIds = (this.#resolvedExtras[context] || []).map((e) => e.id).join(',');
            if (previousIds !== currentIds)
                await this.invokeWrapperAction(context, Dial.TYPES.LONG_TAP);
            else if (previousIds.length > 0)
                await this.softRefresh(context);
        }
        async fetchItems(page, context) {
            await this.#refreshResolvedExtras(context);
            const extras = this.#resolvedExtras[context] || [];
            const pageStart = (page - 1) * constants.WRAPPER_ITEMS_PER_PAGE;
            if (extras.length > 0 && this.#lastSpotifyTotal > 0 && pageStart >= this.#lastSpotifyTotal) {
                const extrasStart = pageStart - this.#lastSpotifyTotal;
                return {
                    status: constants.WRAPPER_RESPONSE_SUCCESS,
                    items: extras.slice(extrasStart, extrasStart + constants.WRAPPER_ITEMS_PER_PAGE),
                    total: this.#lastSpotifyTotal + extras.length
                };
            }
            const result = await wrapper.getUserPlaylists(page);
            if ((!result) || typeof result !== 'object' || (result.status !== constants.WRAPPER_RESPONSE_SUCCESS && result.status !== constants.WRAPPER_RESPONSE_SUCCESS_INDICATIVE))
                return result;
            this.#lastSpotifyTotal = result.total;
            if (extras.length === 0)
                return result;
            const combinedTotal = result.total + extras.length;
            const spotifyItemsOnPage = result.items.length;
            const roomForExtras = constants.WRAPPER_ITEMS_PER_PAGE - spotifyItemsOnPage;
            const items = [...result.items];
            if (roomForExtras > 0) {
                const extrasStart = Math.max(0, pageStart + spotifyItemsOnPage - result.total);
                items.push(...extras.slice(extrasStart, extrasStart + roomForExtras));
            }
            return {
                status: result.status,
                items,
                total: combinedTotal
            };
        }
    });
    return _classThis;
})();

let MyLikedSongs = (() => {
    let _classDecorators = [action({ UUID: 'com.ntanis.essentials-for-spotify.my-liked-songs-dial' })];
    let _classDescriptor;
    let _classExtraInitializers = [];
    let _classThis;
    let _classSuper = ItemsDial;
    (class extends _classSuper {
        static { _classThis = this; }
        static {
            const _metadata = typeof Symbol === "function" && Symbol.metadata ? Object.create(_classSuper[Symbol.metadata] ?? null) : void 0;
            __esDecorate(null, _classDescriptor = { value: _classThis }, _classDecorators, { kind: "class", name: _classThis.name, metadata: _metadata }, null, _classExtraInitializers);
            _classThis = _classDescriptor.value;
            if (_metadata) Object.defineProperty(_classThis, Symbol.metadata, { enumerable: true, configurable: true, writable: true, value: _metadata });
            __runInitializers(_classThis, _classExtraInitializers);
        }
        constructor() {
            super('layouts/items-layout.json', 'images/icons/items.png');
        }
        async playSelectedItem(item) {
            return wrapper.playItem({
                type: 'user',
                id: `${wrapper.user?.id}:collection`
            }, {
                uri: `spotify:track:${item.id}`
            });
        }
        async fetchItems(page, _context) {
            return await wrapper.getUserLikedSongs(page);
        }
    });
    return _classThis;
})();

const ICON_SIZE = 144;
const SLIDER_X = 94;
const SLIDER_WIDTH = 6;
const SLIDER_TOP = 22;
const SLIDER_BOTTOM = 122;
const SLIDER_TRACK_HEIGHT = SLIDER_BOTTOM - SLIDER_TOP;
const KNOB_WIDTH = 16;
const KNOB_HEIGHT = 4;
let SetVolumeButton = (() => {
    let _classDecorators = [action({ UUID: 'com.ntanis.essentials-for-spotify.set-volume-button' })];
    let _classDescriptor;
    let _classExtraInitializers = [];
    let _classThis;
    let _classSuper = Button;
    (class extends _classSuper {
        static { _classThis = this; }
        static {
            const _metadata = typeof Symbol === "function" && Symbol.metadata ? Object.create(_classSuper[Symbol.metadata] ?? null) : void 0;
            __esDecorate(null, _classDescriptor = { value: _classThis }, _classDecorators, { kind: "class", name: _classThis.name, metadata: _metadata }, null, _classExtraInitializers);
            _classThis = _classDescriptor.value;
            if (_metadata) Object.defineProperty(_classThis, Symbol.metadata, { enumerable: true, configurable: true, writable: true, value: _metadata });
        }
        static STATABLE = true;
        #generateImage(volume) {
            const fillHeight = (volume / 100) * SLIDER_TRACK_HEIGHT;
            const fillY = SLIDER_BOTTOM - fillHeight;
            const svg = `
			<svg width="${ICON_SIZE}" height="${ICON_SIZE}" viewBox="0 0 ${ICON_SIZE} ${ICON_SIZE}" xmlns="http://www.w3.org/2000/svg">
				<defs>
					<pattern id="baseIcon" patternUnits="userSpaceOnUse" width="${ICON_SIZE}" height="${ICON_SIZE}">
						<image href="data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAJAAAACQCAYAAADnRuK4AAAACXBIWXMAAAsTAAALEwEAmpwYAAAE8WlUWHRYTUw6Y29tLmFkb2JlLnhtcAAAAAAAPD94cGFja2V0IGJlZ2luPSLvu78iIGlkPSJXNU0wTXBDZWhpSHpyZVN6TlRjemtjOWQiPz4gPHg6eG1wbWV0YSB4bWxuczp4PSJhZG9iZTpuczptZXRhLyIgeDp4bXB0az0iQWRvYmUgWE1QIENvcmUgMTAuMC1jMDAwIDI1LkcuZWY3MmU0ZSwgMjAyNS8wNi8yNy0xODo1NDowNSAgICAgICAgIj4gPHJkZjpSREYgeG1sbnM6cmRmPSJodHRwOi8vd3d3LnczLm9yZy8xOTk5LzAyLzIyLXJkZi1zeW50YXgtbnMjIj4gPHJkZjpEZXNjcmlwdGlvbiByZGY6YWJvdXQ9IiIgeG1sbnM6eG1wPSJodHRwOi8vbnMuYWRvYmUuY29tL3hhcC8xLjAvIiB4bWxuczpkYz0iaHR0cDovL3B1cmwub3JnL2RjL2VsZW1lbnRzLzEuMS8iIHhtbG5zOnBob3Rvc2hvcD0iaHR0cDovL25zLmFkb2JlLmNvbS9waG90b3Nob3AvMS4wLyIgeG1sbnM6eG1wTU09Imh0dHA6Ly9ucy5hZG9iZS5jb20veGFwLzEuMC9tbS8iIHhtbG5zOnN0RXZ0PSJodHRwOi8vbnMuYWRvYmUuY29tL3hhcC8xLjAvc1R5cGUvUmVzb3VyY2VFdmVudCMiIHhtcDpDcmVhdG9yVG9vbD0iQWRvYmUgUGhvdG9zaG9wIDI3LjMgKFdpbmRvd3MpIiB4bXA6Q3JlYXRlRGF0ZT0iMjAyNi0wMi0xOVQyMzozOTowMCswMjowMCIgeG1wOk1vZGlmeURhdGU9IjIwMjYtMDItMTlUMjM6NDk6MTYrMDI6MDAiIHhtcDpNZXRhZGF0YURhdGU9IjIwMjYtMDItMTlUMjM6NDk6MTYrMDI6MDAiIGRjOmZvcm1hdD0iaW1hZ2UvcG5nIiBwaG90b3Nob3A6Q29sb3JNb2RlPSIzIiB4bXBNTTpJbnN0YW5jZUlEPSJ4bXAuaWlkOjNjOTY3Y2Q0LTZjNzQtMTI0Yy1hMTFkLTg5YTg5ZmY1MjY5NiIgeG1wTU06RG9jdW1lbnRJRD0ieG1wLmRpZDozYzk2N2NkNC02Yzc0LTEyNGMtYTExZC04OWE4OWZmNTI2OTYiIHhtcE1NOk9yaWdpbmFsRG9jdW1lbnRJRD0ieG1wLmRpZDozYzk2N2NkNC02Yzc0LTEyNGMtYTExZC04OWE4OWZmNTI2OTYiPiA8eG1wTU06SGlzdG9yeT4gPHJkZjpTZXE+IDxyZGY6bGkgc3RFdnQ6YWN0aW9uPSJjcmVhdGVkIiBzdEV2dDppbnN0YW5jZUlEPSJ4bXAuaWlkOjNjOTY3Y2Q0LTZjNzQtMTI0Yy1hMTFkLTg5YTg5ZmY1MjY5NiIgc3RFdnQ6d2hlbj0iMjAyNi0wMi0xOVQyMzozOTowMCswMjowMCIgc3RFdnQ6c29mdHdhcmVBZ2VudD0iQWRvYmUgUGhvdG9zaG9wIDI3LjMgKFdpbmRvd3MpIi8+IDwvcmRmOlNlcT4gPC94bXBNTTpIaXN0b3J5PiA8L3JkZjpEZXNjcmlwdGlvbj4gPC9yZGY6UkRGPiA8L3g6eG1wbWV0YT4gPD94cGFja2V0IGVuZD0iciI/PrnH654AAAT8SURBVHic7d1PT1xVGMfx55yZgkVom5ShQNNFV/oCpE1M2ii4MnHpa/A1+T5sXFiLdafW7kx0p9JAKRD/0VLQucecuTPtMCnz7zeEGZ7vZ1eakAG+Oefcc+beCYtXrz61EGaCmaWisHYhxmP/7vz/fnR+j1Eb5jWNm3DKv6PTklJ6UTWz+WBW6ecHGccfdBxfkyMz+bd/eNavAhPrMFpK9bN+FZhQKdUZ/yEhIEgICJLIVQwU1AMJAUFCQJDE83AUgLPDCAQJAUFCQJAQECQEBAkBQUJAkBAQJAQECQFBQkCQEBAkvKEMEuqBhIAgISBICAgSAoKEgCAhIEgICBICUgQzuzxl8dObFu8smk01HrPkCgGp8dyuWXh/wezaRZe/TYc/8ojMXbC4Mm9hdcnC5anya8ncyY+4w6Ajz+wFix9dt3jnmlntotlRPT9syTwioGGmrZVaGc/8W+XXHCOgQeSRZ6VmYW3ZrJbjcV4PAfUpNNc8a9ct3m2NPMSTEdAg01Yrnkg8LQTUy1wZT/hwqVww084xBNR12pqyuLZk8e4iC+YTEFDPq61FFsxdsJF44iZhrbFJSDzdMQK9cdpaLhfMrHl6IqDOaesWm4SDYAprySPPe62rLS7V+1XND9l0fW/Yq7Ot5ddnW7TTN99TGGdbMt8B5ZHnVr7a4mxrWD4Daj/bakxbxDOsqt9pa56zrRGourzaytPWB5xtjYKfgHIol6YsrnK2NUpVV/Hc5mxr1HxsAF1qTlutTULiGZmqi03C1eYm4QJrHlcBhXevaH/wSrBwc67tUn2ELw7jH1D87B1tuglmYbpi9naVS3WPAQUus8feeC+i6z5v1psk4x0Qxh4BQUJAkBAQJAQECQFBQkCQEBAkBAQJAUFCQJAQECQEBAkBQUJAkBAQJAQECQFBQkCQEBAkBIRzHFCFe3rG3VjfF2Y7Bz1vCOx140/IH0M5y42FLgOqf/6LNkbGYOHGbOMBmrbAg6TcBZR+/lP7BsEs/bpvNh3LZwLxcAVnayBVnt/2/7VifcuKL59Y2tg3K7jbdZTOd0Atfx9Z8cOOpfUts52Xbj/f9DT4CCj38teRFd/tWPHtU7PtA0aiEfERUHtE9zetuLdh6bd9lx/T7WoRfSrymujHPYspWPikyudiiPyMQG9YExXfbJk9e8l0JvAZUOvq7MFWOZ1tPCeiIfkMqOV5ns52Ld3fLEciHmg1MN8BpeZ09n3z6oxL/IH5DujYdLZpxRe/l1dnTGd9i64/bK5djujxnqUHW+wTDYB6TprOnh0wnfWBgNrlXv5pnp3de1IexDISdUVAJ2427r6ezhiJTkRA3aazvNnI2VlXBNRrOmttNjbOzoioEwH1tdm4Z+nrTbNtjj06EVC/09mjXSsecnbWiYCGmc44O3uFgIY5O/sqn52xsM4IaNjNxofb5XTmfCDy94YyVWqOROubjf2isLZsYTHf7eHzTWkEpGw2Ptot38z48Y3yaw4bIiBpYX3UmM7ilenmmsjcISD5rSD/WfrpD7PDulnhryACUqWk30E7wbgKg4SAICEgSAgIEgKChIAgISBICAgSAoKEgCAhIEgICBICgoSAICEgSAgIEgKChIAgISBICAgSAoKEgCAhIEgICBICgoSAICEgSAgIkphSqmjfAm6FUMkj0PRZvw5MrOn8eJfdVBQzZ/1KYJMnhBf/A4glah62cm9+AAAAAElFTkSuQmCC" x="0" y="0" width="${ICON_SIZE}" height="${ICON_SIZE}"/>
					</pattern>
					<linearGradient id="knobShadow" x1="0" y1="0" x2="0" y2="1">
						<stop offset="0%" stop-color="black" stop-opacity="0"/>
						<stop offset="100%" stop-color="black" stop-opacity="0.8"/>
					</linearGradient>
				</defs>

				<rect width="${ICON_SIZE}" height="${ICON_SIZE}" fill="url(#baseIcon)"/>
				<rect x="${SLIDER_X}" y="${SLIDER_TOP}" width="${SLIDER_WIDTH}" height="${SLIDER_TRACK_HEIGHT}" rx="${SLIDER_WIDTH / 2}" fill="#333333"/>
				<rect x="${SLIDER_X}" y="${fillY}" width="${SLIDER_WIDTH}" height="${fillHeight}" rx="${SLIDER_WIDTH / 2}" fill="#1db954"/>
				<rect x="${SLIDER_X + (SLIDER_WIDTH / 2) - (KNOB_WIDTH / 2)}" y="${fillY - 8}" width="${KNOB_WIDTH}" height="8" rx="1" fill="url(#knobShadow)"/>
				<rect x="${SLIDER_X + (SLIDER_WIDTH / 2) - (KNOB_WIDTH / 2)}" y="${fillY - (KNOB_HEIGHT / 2)}" width="${KNOB_WIDTH}" height="${KNOB_HEIGHT}" rx="${KNOB_HEIGHT / 2}" fill="#ffffff"/>
			</svg>
		`;
            return `data:image/svg+xml;base64,${Buffer.from(svg).toString('base64')}`;
        }
        async #updateImage(context) {
            const volume = this.settings[context].volume ?? 50;
            await this.setImage(context, this.#generateImage(volume));
        }
        async invokeWrapperAction(context, type) {
            if (type === Button.TYPES.RELEASED)
                return;
            if (wrapper.volumePercent === null)
                return constants.WRAPPER_RESPONSE_NOT_AVAILABLE;
            const response = await wrapper.setVolume(this.settings[context].volume);
            if (response === constants.WRAPPER_RESPONSE_SUCCESS)
                return constants.WRAPPER_RESPONSE_SUCCESS_INDICATIVE;
            return response;
        }
        async onSettingsUpdated(context, oldSettings) {
            await super.onSettingsUpdated(context, oldSettings);
            if (this.settings[context].volume === undefined)
                await this.setSettings(context, { volume: 50 });
            await this.#updateImage(context);
        }
        async onStateSettled(context) {
            await super.onStateSettled(context, true);
            await this.#updateImage(context);
        }
        static {
            __runInitializers(_classThis, _classExtraInitializers);
        }
    });
    return _classThis;
})();

let AddToPlaylistButton = (() => {
    let _classDecorators = [action({ UUID: 'com.ntanis.essentials-for-spotify.add-to-playlist-button' })];
    let _classDescriptor;
    let _classExtraInitializers = [];
    let _classThis;
    let _classSuper = Button;
    (class extends _classSuper {
        static { _classThis = this; }
        static {
            const _metadata = typeof Symbol === "function" && Symbol.metadata ? Object.create(_classSuper[Symbol.metadata] ?? null) : void 0;
            __esDecorate(null, _classDescriptor = { value: _classThis }, _classDecorators, { kind: "class", name: _classThis.name, metadata: _metadata }, null, _classExtraInitializers);
            _classThis = _classDescriptor.value;
            if (_metadata) Object.defineProperty(_classThis, Symbol.metadata, { enumerable: true, configurable: true, writable: true, value: _metadata });
        }
        static STATABLE = true;
        #cachedPlaylist = {};
        constructor() {
            super();
            this.setStatelessImage('images/states/add-to-playlist-unknown');
            wrapper.on('songChanged', this.#onSongChanged.bind(this));
        }
        async #onSongChanged(song, pending = false) {
            const promises = [];
            for (const context of this.contexts)
                promises.push(new Promise(async (resolve) => {
                    if ((!song) || pending) {
                        this.clearMarquee(context);
                        await this.setTitle(context, '');
                        await this.setImage(context, pending ? 'images/states/pending' : 'images/states/add-to-playlist-unknown');
                        this.setUnpressable(context, true);
                    }
                    else {
                        this.setUnpressable(context, false);
                        await this.#updateDisplay(context);
                    }
                    resolve(true);
                }));
            await Promise.allSettled(promises);
        }
        #processImagePlus(iconDataUrl) {
            const iconSize = 120;
            const badgeSize = 36;
            const badgeX = iconSize - badgeSize - 6;
            const badgeY = iconSize - badgeSize - 6;
            const svg = `
			<svg width="${iconSize}" height="${iconSize}" viewBox="0 0 ${iconSize} ${iconSize}" xmlns="http://www.w3.org/2000/svg">
			
				<defs>
					<pattern id="iconPattern" patternUnits="userSpaceOnUse" width="${iconSize}" height="${iconSize}">
						<image href="${iconDataUrl}" x="0" y="0" width="${iconSize}" height="${iconSize}"/>
					</pattern>
				</defs>
				
				<rect width="${iconSize}" height="${iconSize}" fill="url(#iconPattern)"/>
				<circle cx="${badgeX + badgeSize / 2}" cy="${badgeY + badgeSize / 2}" r="${badgeSize / 2}" fill="#1db954" stroke="#191414" stroke-width="2"/>
				<line x1="${badgeX + badgeSize / 2}" y1="${badgeY + 9}" x2="${badgeX + badgeSize / 2}" y2="${badgeY + badgeSize - 9}" stroke="#191414" stroke-width="3" stroke-linecap="round"/>
				<line x1="${badgeX + 9}" y1="${badgeY + badgeSize / 2}" x2="${badgeX + badgeSize - 9}" y2="${badgeY + badgeSize / 2}" stroke="#191414" stroke-width="3" stroke-linecap="round"/>
				
			</svg>
		`;
            return `data:image/svg+xml;base64,${Buffer.from(svg).toString('base64')}`;
        }
        async #updateImage(context) {
            if (!this.#cachedPlaylist[context]) {
                await this.setImage(context, 'images/states/add-to-playlist-unknown');
                return;
            }
            if (!images.isItemCached(this.#cachedPlaylist[context]))
                await this.setImage(context, 'images/states/pending');
            const image = await images.getForItem(this.#cachedPlaylist[context]);
            if (image)
                await this.setImage(context, this.#processImagePlus(`data:image/jpeg;base64,${image}`));
            else
                await this.setImage(context, 'images/states/add-to-playlist');
        }
        async #updateDisplay(context) {
            const show = this.settings[context].show || ['title'];
            const data = [];
            let needsRestart = !this.marquees[context];
            if (show.includes('title') && this.#cachedPlaylist[context]?.title)
                data.push({
                    key: 'title',
                    value: this.#cachedPlaylist[context].title
                });
            if (data.length === 0) {
                this.clearMarquee(context);
                await this.setTitle(context, '');
            }
            else if (needsRestart || (!this.marquees[context]))
                await this.marqueeTitle('add-to-playlist', data, context);
            await this.#updateImage(context);
        }
        async #resolvePlaylist(context) {
            const spotify_url = this.settings[context].spotify_url;
            const badUrl = !/^https?:\/\/open\.spotify\.com\/(?:intl-[a-z]{2}\/)?playlist\/[A-Za-z0-9]{22}(?:\/)?(?:\?.*)?$/.test(spotify_url);
            if ((!spotify_url) || badUrl) {
                this.#cachedPlaylist[context] = null;
                return;
            }
            if (this.#cachedPlaylist[context]?.url === spotify_url)
                return;
            this.#cachedPlaylist[context] = await wrapper.getInformationOnUrl(spotify_url);
        }
        async invokeWrapperAction(context, type) {
            if (type === Button.TYPES.RELEASED)
                return;
            if ((!this.#cachedPlaylist[context]?.id) || wrapper.song?.item.uri.includes('local:'))
                return constants.WRAPPER_RESPONSE_NOT_AVAILABLE;
            const currentTrack = await wrapper.getCurrentTrack();
            if (!currentTrack?.uri) {
                if (!wrapper.song?.item?.id)
                    return constants.WRAPPER_RESPONSE_NOT_AVAILABLE;
                const response = await wrapper.addSongToPlaylist(this.#cachedPlaylist[context].id, wrapper.song.item.uri);
                if (response === constants.WRAPPER_RESPONSE_SUCCESS)
                    return constants.WRAPPER_RESPONSE_SUCCESS_INDICATIVE;
                else
                    return response;
            }
            const response = await wrapper.addSongToPlaylist(this.#cachedPlaylist[context].id, currentTrack.uri);
            if (response === constants.WRAPPER_RESPONSE_SUCCESS)
                return constants.WRAPPER_RESPONSE_SUCCESS_INDICATIVE;
            else
                return response;
        }
        async onSettingsUpdated(context, oldSettings) {
            await super.onSettingsUpdated(context, oldSettings);
            if (!this.settings[context].show)
                await this.setSettings(context, {
                    show: ['title']
                });
            const urlChanged = oldSettings.spotify_url !== this.settings[context].spotify_url;
            const showChanged = oldSettings.show?.length !== this.settings[context].show?.length || (oldSettings.show && this.settings[context].show && (!oldSettings.show.every((value, index) => value === this.settings[context].show[index])));
            if (urlChanged) {
                this.clearMarquee(context);
                await this.#resolvePlaylist(context);
            }
            if (urlChanged || showChanged) {
                if (showChanged)
                    this.clearMarquee(context);
                await this.#updateDisplay(context);
            }
        }
        async onStateSettled(context) {
            await super.onStateSettled(context, true);
            await this.#resolvePlaylist(context);
            if (wrapper.song) {
                this.setUnpressable(context, false);
                await this.#updateDisplay(context);
            }
            else
                await this.#onSongChanged(null, false);
        }
        async onStateLoss(context) {
            await super.onStateLoss(context);
            this.clearMarquee(context);
            await this.setTitle(context, '');
        }
        static {
            __runInitializers(_classThis, _classExtraInitializers);
        }
    });
    return _classThis;
})();

var actions = {
	register: () => {
		streamDeck.actions.registerAction(new SetupButton());
		streamDeck.actions.registerAction(new PlayPauseButton());
		streamDeck.actions.registerAction(new PreviousSongButton());
		streamDeck.actions.registerAction(new NextSongButton());
		streamDeck.actions.registerAction(new BackwardSeekButton());
		streamDeck.actions.registerAction(new ForwardSeekButton());
		streamDeck.actions.registerAction(new ShuffleButton());
		streamDeck.actions.registerAction(new LoopContextButton());
		streamDeck.actions.registerAction(new LoopSongButton());
		streamDeck.actions.registerAction(new ModeStackButton());
		streamDeck.actions.registerAction(new LikeUnlikeButton());
		streamDeck.actions.registerAction(new SongExplicitButton());
		streamDeck.actions.registerAction(new VolumeUpButton());
		streamDeck.actions.registerAction(new VolumeDownButton());
		streamDeck.actions.registerAction(new VolumeMuteUnmuteButton());
		streamDeck.actions.registerAction(new VolumeStackButton());
		streamDeck.actions.registerAction(new PlayContextButton());
		streamDeck.actions.registerAction(new SongStackButton());
		streamDeck.actions.registerAction(new SongClipboardButton());
		streamDeck.actions.registerAction(new ContextInformationButton());
		streamDeck.actions.registerAction(new TransferPlaybackButton());
		streamDeck.actions.registerAction(new UserInformationButton());
		streamDeck.actions.registerAction(new VolumeControlDial());
		streamDeck.actions.registerAction(new PlaybackControlDial());
		streamDeck.actions.registerAction(new MyPlaylistsDial());
		streamDeck.actions.registerAction(new MyLikedSongs());
		streamDeck.actions.registerAction(new SetVolumeButton());
		streamDeck.actions.registerAction(new AddToPlaylistButton());
	}
};

https.globalAgent.keepAlive = true;
https.globalAgent.keepAliveMsecs = 60000;

streamDeck.connect().then(() => {
	logger.info('Connected to Stream Deck.');

	streamDeck.settings.getGlobalSettings().then(settings => {
		overlayServer.start();

		if (settings.clientId && settings.clientSecret && settings.refreshToken) {
			logger.info('Found global settings.');
			connector.startSetup(settings.clientId, settings.clientSecret, settings.refreshToken, settings.lastDeviceId);
		} else {
			logger.info('No global settings found.');
			connector.startSetup();
		}
	}).catch(e => {
		logger.error(`An error occurred while getting the Stream Deck global settings: "${e.message || 'No message.'}" @ "${e.stack || 'No stack trace.'}".`);
		overlayServer.start();
		connector.startSetup();
	});

	actions.register();
}).catch(e => logger.error(`An error occured while connecting to Stream Deck: "${e.message || 'No message.'}" @ "${e.stack || 'No stack trace.'}".`));

process.on('uncaughtException', e => logger.error(`An uncaught exception occured: "${e.message || 'No message.'}" @ "${e.stack || 'No stack trace.'}".`));
process.on('unhandledRejection', e => logger.error(`An unhandled promise rejection occured: "${e.message || 'No message.'}" @ "${e.stack || 'No stack trace.'}".`));
//# sourceMappingURL=plugin.js.map
