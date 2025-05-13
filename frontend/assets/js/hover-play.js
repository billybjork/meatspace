export const HoverPlay = {
    mounted() {
      this.el.addEventListener("mouseenter", () => {
        this.el.play();
      });
  
      this.el.addEventListener("mouseleave", () => {
        this.el.pause();
        this.el.currentTime = 0;
      });
    }
  };  