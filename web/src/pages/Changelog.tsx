import { useEffect, useState } from 'react';
import { marked } from 'marked';
import hljs from 'highlight.js';
import 'highlight.js/styles/github-dark.css';
import { motion } from 'framer-motion';

export default function Changelog() {
    const [html, setHtml] = useState('');

    useEffect(() => {
        const fetchChangelog = async () => {
            try {
                const res = await fetch('/CHANGELOG.md');
                const text = await res.text();
                marked.use({ gfm: true, breaks: true });
                setHtml(await marked.parse(text));
            } catch (err) {
                setHtml('<h1>Changelog not found</h1>');
            }
        };
        fetchChangelog();
    }, []);

    useEffect(() => {
        if (html) hljs.highlightAll();
    }, [html]);

    return (
        <div className="flex-1 pt-32 pb-20 px-6 max-w-4xl mx-auto w-full">
            <div className="mb-16 text-center">
                <h1 className="text-5xl font-black text-white mb-6 tracking-tight">Changelog</h1>
                <p className="text-lg text-zinc-400 font-medium">Tracking the evolution of NyroDB.</p>
            </div>

            <motion.div
                initial={{ opacity: 0, y: 10 }}
                animate={{ opacity: 1, y: 0 }}
                transition={{ duration: 0.3 }}
                className="bg-zinc-900/30 border border-zinc-800/40 rounded-2xl p-8 md:p-12 shadow-xl"
            >
                <div className="prose prose-invert prose-zinc max-w-none">
                    <div dangerouslySetInnerHTML={{ __html: html }} />
                </div>
            </motion.div>
        </div>
    );
}
