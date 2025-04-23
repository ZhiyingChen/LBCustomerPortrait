from src.gui.hybrid import HybridApp





if __name__ == "__main__":
    # 替换原有主函数
    app = HybridApp()
    app.geometry(f"{app.winfo_screenwidth()}x{app.winfo_screenheight()}")
    app.mainloop()
