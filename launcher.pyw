#!/usr/bin/env python3
"""
Snowflake Procedure Monitor - GUI Launcher
A cozy pixelated GUI for launching and monitoring the Snowflake procedure monitor.
"""

import tkinter as tk
from tkinter import font as tkfont
import threading
import sys
import os
import time

# Add project paths
project_root = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(project_root, 'src'))
sys.path.insert(0, os.path.join(project_root, 'config'))

# Snowflake brand colors - Cozy pixel palette
SNOWFLAKE_BLUE = "#29B5E8"
SNOWFLAKE_DARK = "#1A7BA8"
SNOWFLAKE_DARKER = "#0F4C66"
SNOWFLAKE_LIGHT = "#E8F7FC"
BG_COLOR = "#1E2A3A"
BG_DARKER = "#151D26"
TEXT_COLOR = "#FFFFFF"
SUCCESS_COLOR = "#4ADE80"
ERROR_COLOR = "#F87171"
WARNING_COLOR = "#FBBF24"
PIXEL_BORDER = "#0D1B2A"


class SnowflakeMonitorGUI:
    """GUI for the Snowflake Procedure Monitor."""
    
    def __init__(self):
        # Create hidden root window (this shows in taskbar)
        self.hidden_root = tk.Tk()
        self.hidden_root.attributes('-alpha', 0)  # Invisible
        self.hidden_root.title("Snowflake Monitor")
        self.hidden_root.iconify()  # Minimize hidden root
        
        # Create actual window as Toplevel (inherits taskbar presence)
        self.root = tk.Toplevel(self.hidden_root)
        self.root.title("Snowflake Monitor")
        
        # Remove default Windows title bar for custom styling
        self.root.overrideredirect(True)
        
        # Window size
        self.width = 380
        self.height = 260
        
        # Center window on screen
        x = (self.root.winfo_screenwidth() // 2) - (self.width // 2)
        y = (self.root.winfo_screenheight() // 2) - (self.height // 2)
        self.root.geometry(f"{self.width}x{self.height}+{x}+{y}")
        self.root.configure(bg=PIXEL_BORDER)
        
        # Make window appear in taskbar (Windows specific)
        self.root.wm_attributes('-topmost', False)
        self.hidden_root.bind('<Map>', lambda e: self.root.deiconify())
        self.hidden_root.bind('<Unmap>', lambda e: self.root.withdraw())
        
        # Monitor state
        self.monitor = None
        self.monitor_thread = None
        self.running = False
        self.snowflake_session_id = None
        
        # For window dragging
        self._drag_x = 0
        self._drag_y = 0
        
        # Create UI
        self._create_ui()
        
        # Start monitor automatically
        self.root.after(500, self.start_monitor)
    
    def _get_pixel_font(self, size, bold=False):
        """Get the best available pixel-style font."""
        # Priority: Pixel fonts > Monospace fonts
        # These fonts give a cozy pixel aesthetic
        pixel_fonts = [
            "Silkscreen",      # Google Font - very pixel
            "Press Start 2P",  # Google Font - classic pixel
            "VT323",           # Google Font - terminal pixel
            "Pixelify Sans",   # Google Font - modern pixel
            "Terminal",        # Windows built-in
            "Fixedsys",        # Windows classic
            "Consolas",        # Good monospace fallback
            "Courier New"      # Universal fallback
        ]
        
        weight = "bold" if bold else "normal"
        
        for font_name in pixel_fonts:
            try:
                test_font = tkfont.Font(family=font_name, size=size, weight=weight)
                # Check if font actually exists
                if test_font.actual()['family'].lower() == font_name.lower():
                    return test_font
            except:
                continue
        
        # Final fallback
        return tkfont.Font(family="Consolas", size=size, weight=weight)
    
    def _create_ui(self):
        """Create the user interface."""
        # Fonts - larger sizes for better readability
        self.font_title = self._get_pixel_font(12, bold=True)
        self.font_normal = self._get_pixel_font(12)
        self.font_small = self._get_pixel_font(10)
        
        # Main container with pixel border effect
        self.main_container = tk.Frame(self.root, bg=BG_COLOR)
        self.main_container.pack(fill=tk.BOTH, expand=True, padx=2, pady=2)
        
        # Custom title bar
        self.title_bar = tk.Frame(self.main_container, bg=SNOWFLAKE_DARK, height=32)
        self.title_bar.pack(fill=tk.X)
        self.title_bar.pack_propagate(False)
        
        # Title bar - left side (title)
        self.title_label = tk.Label(
            self.title_bar,
            text="SNOWFLAKE MONITOR",
            font=self.font_title,
            bg=SNOWFLAKE_DARK,
            fg=TEXT_COLOR
        )
        self.title_label.pack(side=tk.LEFT, padx=12)
        
        # Title bar - right side (close button)
        self.close_btn = tk.Label(
            self.title_bar,
            text=" X ",
            font=self.font_title,
            bg=SNOWFLAKE_DARK,
            fg=TEXT_COLOR,
            cursor="hand2"
        )
        self.close_btn.pack(side=tk.RIGHT, padx=4)
        self.close_btn.bind("<Button-1>", lambda e: self.on_close())
        self.close_btn.bind("<Enter>", lambda e: self.close_btn.configure(bg=ERROR_COLOR))
        self.close_btn.bind("<Leave>", lambda e: self.close_btn.configure(bg=SNOWFLAKE_DARK))
        
        # Minimize button
        self.min_btn = tk.Label(
            self.title_bar,
            text=" - ",
            font=self.font_title,
            bg=SNOWFLAKE_DARK,
            fg=TEXT_COLOR,
            cursor="hand2"
        )
        self.min_btn.pack(side=tk.RIGHT, padx=0)
        self.min_btn.bind("<Button-1>", self._minimize_window)
        self.min_btn.bind("<Enter>", lambda e: self.min_btn.configure(bg=SNOWFLAKE_DARKER))
        self.min_btn.bind("<Leave>", lambda e: self.min_btn.configure(bg=SNOWFLAKE_DARK))
        
        # Enable window dragging from title bar
        self.title_bar.bind("<Button-1>", self._start_drag)
        self.title_bar.bind("<B1-Motion>", self._on_drag)
        self.title_label.bind("<Button-1>", self._start_drag)
        self.title_label.bind("<B1-Motion>", self._on_drag)
        
        # Main content frame
        self.content_frame = tk.Frame(self.main_container, bg=BG_COLOR)
        self.content_frame.pack(fill=tk.BOTH, expand=True, padx=16, pady=16)
        
        # Progress bar container with pixel border
        self.progress_outer = tk.Frame(self.content_frame, bg=PIXEL_BORDER)
        self.progress_outer.pack(fill=tk.X, pady=(0, 12))
        
        self.progress_inner = tk.Frame(self.progress_outer, bg=BG_DARKER)
        self.progress_inner.pack(fill=tk.X, padx=2, pady=2)
        
        # Custom pixel progress bar - adjusted to fit window
        progress_bar_width = self.width - 44  # Account for padding
        self.progress_canvas = tk.Canvas(
            self.progress_inner,
            width=progress_bar_width,
            height=28,
            bg=BG_DARKER,
            highlightthickness=0
        )
        self.progress_canvas.pack(padx=2, pady=2)
        
        # Progress bar segments (pixel blocks with gaps)
        self.progress_segments = []
        self.num_segments = 17
        gap = 4
        segment_width = (progress_bar_width - 8 - (self.num_segments - 1) * gap) // self.num_segments
        start_x = 4
        
        for i in range(self.num_segments):
            x1 = start_x + i * (segment_width + gap)
            # Draw pixel block with inner shadow effect
            rect = self.progress_canvas.create_rectangle(
                x1, 4, x1 + segment_width, 24,
                fill=BG_DARKER,
                outline=SNOWFLAKE_DARKER,
                width=1
            )
            self.progress_segments.append(rect)
        
        # Status label
        self.status_label = tk.Label(
            self.content_frame,
            text="Iniciando...",
            font=self.font_normal,
            bg=BG_COLOR,
            fg=WARNING_COLOR
        )
        self.status_label.pack(pady=(12, 6))
        
        # Session ID frame (label + copy button)
        self.session_frame = tk.Frame(self.content_frame, bg=BG_COLOR)
        self.session_frame.pack(pady=(0, 14))
        
        # Session ID label - medium font
        self.session_label = tk.Label(
            self.session_frame,
            text="",
            font=self._get_pixel_font(11),
            bg=BG_COLOR,
            fg=SNOWFLAKE_LIGHT
        )
        self.session_label.pack(side=tk.LEFT)
        
        # Copy button (hidden initially)
        self.copy_btn = tk.Label(
            self.session_frame,
            text=" [COPIAR] ",
            font=self.font_small,
            bg=BG_COLOR,
            fg=SNOWFLAKE_BLUE,
            cursor="hand2"
        )
        self.copy_btn.bind("<Button-1>", self._copy_session_id)
        self.copy_btn.bind("<Enter>", lambda e: self.copy_btn.configure(fg=SUCCESS_COLOR))
        self.copy_btn.bind("<Leave>", lambda e: self.copy_btn.configure(fg=SNOWFLAKE_BLUE))
        
        # Stop button with pixel style
        self.button_frame = tk.Frame(self.content_frame, bg=PIXEL_BORDER)
        self.button_frame.pack(pady=(4, 0))
        
        self.stop_button = tk.Button(
            self.button_frame,
            text="  DETENER  ",
            font=self.font_normal,
            bg=SNOWFLAKE_DARK,
            fg=TEXT_COLOR,
            activebackground=ERROR_COLOR,
            activeforeground=TEXT_COLOR,
            relief=tk.FLAT,
            bd=0,
            padx=16,
            pady=6,
            cursor="hand2",
            command=self.stop_monitor
        )
        self.stop_button.pack(padx=2, pady=2)
        
        # Hover effects for button
        self.stop_button.bind("<Enter>", lambda e: self.stop_button.configure(bg=ERROR_COLOR))
        self.stop_button.bind("<Leave>", lambda e: self.stop_button.configure(bg=SNOWFLAKE_DARK))
    
    def _start_drag(self, event):
        """Start window drag."""
        self._drag_x = event.x
        self._drag_y = event.y
    
    def _on_drag(self, event):
        """Handle window dragging."""
        x = self.root.winfo_x() + (event.x - self._drag_x)
        y = self.root.winfo_y() + (event.y - self._drag_y)
        self.root.geometry(f"+{x}+{y}")
    
    def _minimize_window(self, event=None):
        """Minimize window to taskbar."""
        self.root.withdraw()
        self.hidden_root.iconify()
        # Restore when clicking on taskbar
        self.hidden_root.after(100, self._setup_restore)
    
    def _setup_restore(self):
        """Setup restore from taskbar."""
        def restore(event):
            self.hidden_root.deiconify()
            self.root.deiconify()
            self.root.lift()
        self.hidden_root.bind('<Map>', restore)
    
    def _copy_session_id(self, event=None):
        """Copy session ID to clipboard."""
        if self.snowflake_session_id:
            self.root.clipboard_clear()
            self.root.clipboard_append(self.snowflake_session_id)
            # Visual feedback
            original_text = self.copy_btn.cget("text")
            self.copy_btn.configure(text=" [COPIADO] ", fg=SUCCESS_COLOR)
            self.root.after(1500, lambda: self.copy_btn.configure(text=original_text, fg=SNOWFLAKE_BLUE))
    
    def update_progress(self, value, max_value=100):
        """Update the progress bar (0-100)."""
        filled = int((value / max_value) * self.num_segments)
        for i, segment in enumerate(self.progress_segments):
            if i < filled:
                # Filled segment with gradient effect
                self.progress_canvas.itemconfig(segment, fill=SNOWFLAKE_BLUE, outline=SNOWFLAKE_DARK)
            else:
                # Empty segment
                self.progress_canvas.itemconfig(segment, fill=BG_DARKER, outline=SNOWFLAKE_DARKER)
        self.root.update_idletasks()
    
    def update_status(self, text, color=WARNING_COLOR):
        """Update the status label."""
        self.status_label.configure(text=text, fg=color)
        self.root.update_idletasks()
    
    def update_session(self, session_id):
        """Update the session ID display."""
        if session_id:
            self.session_label.configure(text=f"Session: {session_id}")
            self.copy_btn.pack(side=tk.LEFT, padx=(4, 0))  # Show copy button
        else:
            self.session_label.configure(text="")
            self.copy_btn.pack_forget()  # Hide copy button
        self.root.update_idletasks()
    
    def animate_progress(self, start, end, steps=10, delay=50):
        """Animate progress bar from start to end."""
        current = start
        step_size = (end - start) / steps
        
        def step():
            nonlocal current
            if (step_size > 0 and current < end) or (step_size < 0 and current > end):
                current += step_size
                self.update_progress(current)
                self.root.after(delay, step)
            else:
                self.update_progress(end)
        
        step()
    
    def start_monitor(self):
        """Start the monitor in a separate thread."""
        self.running = True
        self.monitor_thread = threading.Thread(target=self._run_monitor, daemon=True)
        self.monitor_thread.start()
    
    def _run_monitor(self):
        """Run the monitor (in background thread)."""
        try:
            # Phase 1: Initializing
            self.root.after(0, lambda: self.update_status("Iniciando...", WARNING_COLOR))
            self.root.after(0, lambda: self.animate_progress(0, 20))
            
            # Import and initialize
            from monitor import SnowflakeProcedureMonitor
            from config import CHECK_INTERVAL
            
            time.sleep(0.5)
            self.root.after(0, lambda: self.animate_progress(20, 40))
            
            # Phase 2: Creating monitor
            self.root.after(0, lambda: self.update_status("Creando monitor...", WARNING_COLOR))
            self.monitor = SnowflakeProcedureMonitor()
            
            time.sleep(0.3)
            self.root.after(0, lambda: self.animate_progress(40, 60))
            
            # Phase 3: Connecting to Snowflake
            self.root.after(0, lambda: self.update_status("Conectando a Snowflake...", WARNING_COLOR))
            
            if not self.monitor.connect_to_snowflake():
                self.root.after(0, lambda: self.update_status("Error de conexion", ERROR_COLOR))
                self.root.after(0, lambda: self.update_progress(0))
                return
            
            # Get Snowflake session ID
            try:
                cursor = self.monitor.snowflake_conn.cursor()
                cursor.execute("SELECT CURRENT_SESSION()")
                session_result = cursor.fetchone()
                if session_result:
                    self.snowflake_session_id = str(session_result[0])
                    self.root.after(0, lambda: self.update_session(self.snowflake_session_id))
                cursor.close()
            except:
                pass
            
            self.root.after(0, lambda: self.animate_progress(60, 100))
            time.sleep(0.3)
            
            # Phase 4: Monitor active
            self.root.after(0, lambda: self.update_status("Monitor Activo", SUCCESS_COLOR))
            
            # Main monitoring loop
            iteration = 0
            while self.running:
                try:
                    # Process procedures
                    new_count = self.monitor.process_completed_procedures()
                    iteration += 1
                    
                    # Cleanup every 100 iterations
                    if iteration % 100 == 0:
                        self.monitor.cleanup_old_records()
                    
                    # Wait for next check
                    for _ in range(CHECK_INTERVAL):
                        if not self.running:
                            break
                        time.sleep(1)
                        
                except Exception as e:
                    if self.running:
                        error_msg = str(e)[:25] if len(str(e)) > 25 else str(e)
                        self.root.after(0, lambda msg=error_msg: self.update_status(f"Error: {msg}", ERROR_COLOR))
                        time.sleep(5)
            
        except Exception as e:
            error_msg = str(e)[:25] if len(str(e)) > 25 else str(e)
            self.root.after(0, lambda msg=error_msg: self.update_status(f"Error: {msg}", ERROR_COLOR))
        finally:
            self._cleanup()
    
    def _cleanup(self):
        """Clean up resources."""
        if self.monitor:
            try:
                self.monitor.disconnect_from_snowflake()
            except:
                pass
            self.monitor = None
    
    def stop_monitor(self):
        """Stop the monitor and close the application."""
        self.running = False
        self.update_status("Deteniendo...", WARNING_COLOR)
        self.animate_progress(100, 0)
        
        # Give thread time to finish
        self.root.after(1000, self._finish_stop)
    
    def _finish_stop(self):
        """Finish stopping and close window."""
        self._cleanup()
        self.root.destroy()
        self.hidden_root.destroy()
    
    def on_close(self):
        """Handle window close event."""
        self.stop_monitor()
    
    def run(self):
        """Start the GUI main loop."""
        self.hidden_root.mainloop()


def main():
    """Main entry point."""
    app = SnowflakeMonitorGUI()
    app.run()


if __name__ == "__main__":
    main()

