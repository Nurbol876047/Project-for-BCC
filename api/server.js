const express = require('express');
const path = require('path');
const bodyParser = require('body-parser');
const cors = require('cors');
const nodemailer = require('nodemailer');

const app = express();
const PORT = process.env.PORT || 3000;

// Middleware
app.use(cors());
app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: true }));

// Статические файлы (HTML, CSS, JS, изображения)
app.use(express.static(path.join(__dirname)));

// Главная страница
app.get('/', (req, res) => {
    res.sendFile(path.join(__dirname, 'index.html'));
});

// Страница анализа банковских продуктов
app.get('/bank-analysis', (req, res) => {
    res.sendFile(path.join(__dirname, 'bank-analysis.html'));
});

// API для отправки email (форма контактов)
app.post('/api/contact', async (req, res) => {
    try {
        const { name, phone, email, message } = req.body;
        
        // Создаем транспортер для отправки email
        const transporter = nodemailer.createTransporter({
            service: 'gmail', // или другой email сервис
            auth: {
                user: process.env.EMAIL_USER || 'your-email@gmail.com',
                pass: process.env.EMAIL_PASS || 'your-app-password'
            }
        });

        // Настройки письма
        const mailOptions = {
            from: process.env.EMAIL_USER || 'your-email@gmail.com',
            to: 'info@toi-organizer.kz', // ваш email для получения заявок
            subject: 'Новая заявка с сайта Той',
            html: `
                <h2>Новая заявка с сайта</h2>
                <p><strong>Имя:</strong> ${name}</p>
                <p><strong>Телефон:</strong> ${phone}</p>
                <p><strong>Email:</strong> ${email || 'Не указан'}</p>
                <p><strong>Сообщение:</strong></p>
                <p>${message}</p>
            `
        };

        // Отправляем email
        await transporter.sendMail(mailOptions);
        
        res.json({ 
            success: true, 
            message: 'Сіздің өтінішіңіз сәтті жіберілді! Жақын арада сізбен байланысамыз.' 
        });
        
    } catch (error) {
        console.error('Ошибка отправки email:', error);
        res.json({ 
            success: true, 
            message: 'Сіздің өтінішіңіз қабылданды! Жақын арада сізбен байланысамыз.' 
        });
    }
});

// API для получения информации о компании
app.get('/api/about', (req, res) => {
    res.json({
        name: 'Той - Мерекелерді ұйымдастыру',
        description: 'Біз мерекелерді ұйымдастыру нарығында 10 жылдан астам уақыт бойы жұмыс істеп келеміз.',
        services: [
            'Балалар мерекелері',
            'Үйлену тойлары', 
            'Корпоративтер',
            'Музыкалық серіктестік',
            'Кейтеринг',
            'Фото және видео түсіру'
        ],
        contact: {
            phone: '+7 706 640-36-55',
            email: 'info@toi-organizer.kz',
            address: 'Қызылорда қаласы, Kzo hub'
        }
    });
});

// API для получения результатов анализа банковских продуктов
app.get('/api/bank-analysis', (req, res) => {
    try {
        const fs = require('fs');
        const path = require('path');
        
        // Путь к файлу с результатами
        const resultsPath = path.join(__dirname, '../core/output/result.csv');
        
        // Проверяем существование файла
        if (!fs.existsSync(resultsPath)) {
            return res.status(404).json({ 
                error: 'Файл с результатами не найден. Запустите анализ сначала.' 
            });
        }
        
        // Читаем CSV файл
        const csvContent = fs.readFileSync(resultsPath, 'utf8');
        const lines = csvContent.split('\n');
        const headers = lines[0].split(',');
        
        // Парсим данные
        const results = [];
        for (let i = 1; i < lines.length; i++) {
            if (lines[i].trim()) {
                const values = lines[i].split(',');
                const row = {};
                headers.forEach((header, index) => {
                    row[header.trim()] = values[index] ? values[index].trim() : '';
                });
                results.push(row);
            }
        }
        
        // Статистика
        const stats = {
            total_clients: results.length,
            products: {}
        };
        
        results.forEach(row => {
            const product = row.product || row.best_product;  // поддержка нового формата ТЗ
            stats.products[product] = (stats.products[product] || 0) + 1;
        });
        
        res.json({
            success: true,
            data: results,
            statistics: stats
        });
        
    } catch (error) {
        console.error('Ошибка чтения результатов:', error);
        res.status(500).json({ 
            error: 'Ошибка при чтении результатов анализа' 
        });
    }
});

// Обработка 404 ошибки
app.use((req, res) => {
    res.status(404).sendFile(path.join(__dirname, 'index.html'));
});

// Запуск сервера
app.listen(PORT, () => {
    console.log(`🚀 Сервер запущен на порту ${PORT}`);
    console.log(`📱 Откройте браузер и перейдите по адресу: http://localhost:${PORT}`);
    console.log(`📧 Для работы формы контактов настройте переменные окружения EMAIL_USER и EMAIL_PASS`);
});

module.exports = app;
