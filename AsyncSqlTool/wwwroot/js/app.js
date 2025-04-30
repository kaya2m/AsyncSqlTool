window.setupAutoResizeEditor = function () {
    // Memo editörü bulunur
    const editors = document.querySelectorAll('.auto-resize-editor .dxbs-memo-input');

    editors.forEach(editor => {
        // Input alanını izler
        editor.addEventListener('input', function () {
            autoResizeTextarea(this);
        });

        // Sayfa yüklendiğinde de çalıştırır
        setTimeout(() => autoResizeTextarea(editor), 100);
    });

    function autoResizeTextarea(element) {
        // Mevcut yüksekliği sıfırla
        element.style.height = 'auto';

        // Scroll yüksekliğine göre ayarla (min-height ve max-height sınırları CSS'te)
        const newHeight = Math.min(Math.max(element.scrollHeight, 180), 500);
        element.style.height = newHeight + 'px';
    }
};