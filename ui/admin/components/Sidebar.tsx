'use client';

import Link from 'next/link';
import { usePathname } from 'next/navigation';

export default function Sidebar() {
    const pathname = usePathname();

    const links = [
        { href: '/', label: 'Overview', icon: 'M3 3v18h18V3H3zm8 16H5v-6h6v6zm0-8H5V5h6v6zm8 8h-6v-6h6v6zm0-8h-6V5h6v6z' }, // Grid/Dashboard
        { href: '/data', label: 'Data', icon: 'M20 6h-8l-2-2H4c-1.1 0-2 .9-2 2v12c0 1.1.9 2 2 2h16c1.1 0 2-.9 2-2V8c0-1.1-.9-2-2-2zm0 12H4V8h16v10z' }, // Folder
        { href: '/api', label: 'API', icon: 'M20 4H4c-1.1 0-1.99.9-1.99 2L2 18c0 1.1.9 2 2 2h16c1.1 0 2-.9 2-2V6c0-1.1-.9-2-2-2zm-5 14H9v-2h6v2zm0-5H9v-2h6v2zm0-5H9V6h6v2z' }, // Server/API
        { href: '/events', label: 'Events', icon: 'M16 6l2.29 2.29-4.88 4.88-4-4L2 16.59 3.41 18l6-6 4 4 6.3-6.29L22 12V6z' }, // Activity/Bolt
    ];

    return (
        <div className="w-64 h-screen bg-black border-r border-gray-800 flex flex-col">
            <div className="p-6">
                <h1 className="text-xl font-bold text-white tracking-widest">LUMADB</h1>
            </div>
            <nav className="flex-1 px-4 space-y-2">
                {links.map((link) => {
                    const isActive = pathname === link.href || (link.href !== '/' && pathname.startsWith(link.href));
                    return (
                        <Link
                            key={link.href}
                            href={link.href}
                            className={`flex items-center px-4 py-3 rounded-lg transition-colors ${isActive
                                    ? 'bg-blue-600 text-white'
                                    : 'text-gray-400 hover:text-white hover:bg-gray-900'
                                }`}
                        >
                            <svg className="w-5 h-5 mr-3 fill-current" viewBox="0 0 24 24">
                                <path d={link.icon} />
                            </svg>
                            <span className="font-medium">{link.label}</span>
                        </Link>
                    );
                })}
            </nav>
            <div className="p-4 border-t border-gray-800 text-xs text-gray-500">
                v2.0.0
            </div>
        </div>
    );
}
